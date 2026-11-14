"""同步管理器：WS 接收 + 快照衔接 + 断线/丢序自愈。

状态机与不变量（self.cond 底层锁保护 book/synced/current_epoch）：
- 启动 / 断流 / 发现缺号 -> 同一把锁内立即 synced=False（查询返回 503），
  进入新一轮恢复；缺号/断流一经发现，不存在“清理尚未结束仍可查”的窗口；
- 每轮恢复创建一个连接令牌 ConnectionEpoch：先连 WS 缓冲增量，同时取快照；
  快照返回后用缓冲中 seq > snapshot.seq 的增量连续补齐（恰好一次），
  全部衔接才 synced=True；
- 已断连接的快照不得让盘口重新可用，由两道互相补强的闸保证：
  1) 活性闸（主）：接收循环在 recv() 失败/缺号时就地把令牌标 dead
     （一次无锁属性赋值，与事件循环调度先后无关）；快照安装与查询都在
     持锁时核对 current_epoch 身份与 epoch.dead，dead 即拒绝落簿/返回 503；
  2) 闭环守卫（补强）：每轮一个协程经 Condition.wait 从周期开始就挂在
     条件等待队列上（先于任何快照安装排队），连接死亡 notify 后优先于
     安装协程重新拿锁，置 synced=False；进一步消除“安装协程先被唤醒”
     的调度缝隙；
- 每次重连递增 generation 并更换令牌，重连前发起的迟到快照响应一律丢弃，
  不得恢复可用状态或覆盖新盘口；
- 只有“当前存活连接”的快照与连续增量完整衔接后，synced 才会回到 True。
"""
from __future__ import annotations

import asyncio
import json
import logging

import httpx
import websockets

from .config import Settings
from .orderbook import (
    GapError,
    OrderBook,
    ProtocolError,
    StaleSnapshotError,
    build_synced_book,
    validate_increment,
)

log = logging.getLogger("collector.sync")

_CLOSED = object()  # 队列哨兵：WS 接收循环结束


class ConnectionEpoch:
    """一次 WS 连接的活性令牌。

    dead 由接收循环在连接死亡时就地置位（无锁、同步、一次赋值），
    读写双方在持有同步锁时核对其状态与身份。
    """

    __slots__ = ("generation", "dead", "closed")

    def __init__(self, generation: int) -> None:
        self.generation = generation
        self.dead = False
        self.closed = asyncio.Event()  # 供“连接先断则本轮作废”的等待逻辑使用

    def mark_dead(self) -> None:
        if not self.dead:
            self.dead = True
            self.closed.set()


class SyncManager:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.book = OrderBook(symbol=settings.symbol, max_levels=settings.max_levels)
        self.synced = False
        self.generation = 0  # 连接世代号，每轮恢复 +1
        # 单一同步原语：底层锁保护 book/synced/current_epoch；
        # wait/notify 供闭环守卫在连接死亡时优先取得锁。
        self.cond = asyncio.Condition()
        self.lock = self.cond  # 兼容命名：cond 本身就是异步上下文锁
        # 当前恢复周期的连接令牌；None 表示尚无周期
        self.current_epoch: ConnectionEpoch | None = None
        self.last_error: str | None = None
        self.stats: dict[str, int] = {
            "increments_applied": 0,
            "duplicates_skipped": 0,
            "snapshots_applied": 0,
            "stale_snapshots": 0,
            "buffered_applied": 0,
            "buffered_dropped": 0,
            "gaps": 0,
            "resyncs": 0,
        }
        self._task: asyncio.Task | None = None
        self._http: httpx.AsyncClient | None = None

    # ---------- 生命周期 ----------

    async def start(self) -> None:
        self._http = httpx.AsyncClient(
            base_url=self.settings.source_http,
            timeout=self.settings.snapshot_timeout,
        )
        self._task = asyncio.create_task(self._run(), name="sync-manager")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._http is not None:
            await self._http.aclose()

    # ---------- 主循环 ----------

    async def _run(self) -> None:
        while True:
            self.generation += 1
            gen = self.generation
            epoch = ConnectionEpoch(gen)
            async with self.cond:
                self.synced = False
                self.current_epoch = epoch
            try:
                await self._cycle(epoch)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 断线/缺号/协议错误/HTTP 错误都走重同步
                async with self.cond:
                    self.last_error = f"{type(exc).__name__}: {exc}"
                self.stats["resyncs"] += 1
                log.warning("sync cycle failed (gen=%d): %s", gen, self.last_error)
            finally:
                epoch.mark_dead()
                async with self.cond:
                    self.synced = False
            await asyncio.sleep(self.settings.reconnect_delay)

    async def _cycle(self, epoch: ConnectionEpoch) -> None:
        gen = epoch.generation
        queue: asyncio.Queue = asyncio.Queue()
        recv_state: dict[str, object] = {"error": None}

        async with self.cond:
            # 发布本周期令牌（_run 已发布则为幂等覆盖；直接调用 _cycle 的测试同样成立）
            self.current_epoch = epoch
            self.synced = False

        guard_ready = asyncio.Event()

        async def unsync_guard() -> None:
            """闭环守卫：周期内始终优先等待同步锁，连接一死即先于安装协程拿锁。

            通过 cond.wait() 在守卫启动时就进入条件等待队列（早于任何快照
            安装协程排队等锁）；连接死亡 notify_all 后，守卫按等待顺序被
            唤醒并重新申请锁，快照安装协程若已等锁也排在它后面。守卫拿到
            锁即置 synced=False / 令牌 dead，安装协程随后必见 dead 而拒绝落簿。
            """
            async with self.cond:
                guard_ready.set()
                await self.cond.wait_for(lambda: epoch.dead)
                epoch.mark_dead()
                if self.current_epoch is epoch:
                    self.synced = False

        async def receiver(ws) -> None:
            try:
                while True:
                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(), timeout=self.settings.data_timeout
                        )
                    except Exception as exc:
                        # recv() 抛错即连接死亡：就地标令牌 dead（同步、无锁），
                        # 再唤醒闭环守卫去抢占状态锁。
                        recv_state["error"] = exc
                        epoch.mark_dead()
                        async with self.cond:
                            self.cond.notify_all()
                        raise
                    try:
                        msg = json.loads(raw)
                    except (json.JSONDecodeError, TypeError) as exc:
                        raise ProtocolError(f"invalid JSON frame: {exc}")
                    if not isinstance(msg, dict):
                        raise ProtocolError("frame is not an object")
                    mtype = msg.get("type")
                    if mtype == "increment":
                        queue.put_nowait(msg)  # 取快照期间也持续缓冲，一条不丢
                    elif mtype == "heartbeat":
                        continue  # 仅用于保活/断流检测
                    else:
                        raise ProtocolError(f"unknown frame type: {mtype!r}")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if recv_state["error"] is None:
                    recv_state["error"] = exc
            finally:
                # 不直接抢锁：死亡事实由令牌无锁发布 + 守卫走条件队列优先登记
                epoch.mark_dead()
                try:
                    async with self.cond:
                        self.cond.notify_all()
                except (RuntimeError, asyncio.CancelledError):
                    pass
                queue.put_nowait(_CLOSED)

        async with websockets.connect(
            self.settings.source_ws, ping_interval=20, ping_timeout=20, max_size=1 << 20
        ) as ws:
            recv_task = asyncio.create_task(receiver(ws))
            guard_task = asyncio.create_task(unsync_guard())
            # 确保守卫已停在条件等待队列上（早于快照安装可能的等锁），
            # 再发起快照请求 —— 这是“死亡先于安装可见”顺序保证的前提。
            await guard_ready.wait()
            snap_task = asyncio.create_task(self._fetch_snapshot(gen))
            try:
                # 等快照，同时接收循环持续缓冲增量；连接先断则本轮作废
                closed_waiter = asyncio.create_task(epoch.closed.wait())
                await asyncio.wait(
                    {snap_task, closed_waiter}, return_when=asyncio.FIRST_COMPLETED
                )
                closed_waiter.cancel()
                if not snap_task.done():
                    snap_task.cancel()
                    raise ConnectionError("stream closed before snapshot arrived")
                snapshot = snap_task.result()  # HTTP/协议错误在此抛出
                if snapshot is None:
                    raise StaleSnapshotError(f"snapshot of generation {gen} discarded")

                # 排干快照在途期间缓冲的增量
                buffered: list[dict] = []
                while True:
                    try:
                        item = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if item is _CLOSED:
                        await self._mark_unsynced(epoch)
                        raise ConnectionError("stream closed while applying snapshot")
                    buffered.append(item)

                await self._install_snapshot(snapshot, buffered, epoch)

                # 已同步：逐条衔接后续增量
                while True:
                    item = await queue.get()
                    if item is _CLOSED:
                        await self._mark_unsynced(epoch)
                        err = recv_state["error"]
                        raise ConnectionError(
                            f"stream closed: {err}" if err else "stream closed by peer"
                        )
                    await self._apply_increment(item, epoch)
            finally:
                recv_task.cancel()
                guard_task.cancel()
                snap_task.cancel()
                try:
                    await asyncio.gather(
                        recv_task, guard_task, snap_task, return_exceptions=True
                    )
                except asyncio.CancelledError:
                    pass

    async def _mark_unsynced(self, epoch: ConnectionEpoch) -> None:
        """连接死亡路径的统一登记：只操作本周期令牌，绝不误伤新周期。"""
        async with self.cond:
            epoch.mark_dead()
            if self.current_epoch is epoch:
                self.synced = False

    # ---------- 快照 ----------

    async def _fetch_snapshot(self, gen: int) -> dict | None:
        """取快照；返回时若世代已更替（重连前的迟到响应）则丢弃。"""
        assert self._http is not None
        resp = await self._http.get("/snapshot", params={"symbol": self.settings.symbol})
        resp.raise_for_status()
        data = resp.json()
        async with self.cond:
            stale = gen != self.generation
        if stale:
            self.stats["stale_snapshots"] += 1
            log.warning("discarding stale snapshot (gen=%d, current=%d)", gen, self.generation)
            return None
        return data

    async def _install_snapshot(
        self,
        snapshot: dict,
        buffered: list[dict],
        epoch: ConnectionEpoch | None = None,
        gen: int | None = None,
    ) -> None:
        gen = epoch.generation if epoch is not None else gen
        try:
            book, applied, dropped = build_synced_book(
                snapshot, buffered, symbol=self.settings.symbol,
                max_levels=self.settings.max_levels,
            )
        except Exception:
            # 缓冲增量缺号等：衔接失败，保持/恢复失步，交上层重连
            async with self.cond:
                if epoch is None or self.current_epoch is epoch:
                    self.synced = False
            raise
        async with self.cond:
            # 第一道闸：世代号核对，迟到快照绝不覆盖新盘口
            if gen != self.generation:
                self.stats["stale_snapshots"] += 1
                raise StaleSnapshotError(
                    f"refusing to install snapshot of generation {gen} (current {self.generation})"
                )
            if epoch is not None:
                # 第二道闸：令牌已被更换（重连进入新世代）
                if self.current_epoch is not epoch:
                    self.synced = False
                    raise ConnectionError(
                        "refusing to install snapshot: generation superseded by reconnect"
                    )
                # 第三道闸：同一连接已断。守卫通过条件队列优先拿锁置 dead，
                # 安装协程在其后执行，必能看到真实状态。
                if epoch.dead:
                    self.synced = False
                    raise ConnectionError(
                        "refusing to install snapshot: stream already closed"
                    )
            self.book = book
            self.synced = True
            self.stats["snapshots_applied"] += 1
            self.stats["buffered_applied"] += applied
            self.stats["buffered_dropped"] += dropped
            self.last_error = None
        log.info("snapshot installed: seq=%s applied=%d dropped=%d", book.seq, applied, dropped)

    # ---------- 增量 ----------

    async def _apply_increment(self, msg: dict, epoch: ConnectionEpoch | None = None) -> None:
        seq, changes = validate_increment(msg, self.settings.symbol)
        async with self.cond:
            if epoch is not None and (self.current_epoch is not epoch or epoch.dead):
                raise ConnectionError("increment from a closed/superseded connection")
            if not self.synced or self.book.seq is None:
                raise ProtocolError("increment received while unsynced")
            last = self.book.seq
            if seq <= last:
                self.stats["duplicates_skipped"] += 1
                return  # 重复投递：恰好应用一次
            if seq != last + 1:
                # 缺号一经发现：同一次加锁内立即失步，/book 下一次查询即 503，
                # 不需要等待接收循环拆除或外层 finally。
                self.stats["gaps"] += 1
                self.synced = False
                if epoch is not None:
                    epoch.mark_dead()  # 本连接已不可信，推动整轮重连
                raise GapError(f"increment gap: expected seq {last + 1}, got {seq}")
            self.book.apply_increment(seq, changes)
            self.stats["increments_applied"] += 1

    # ---------- 查询视图 ----------

    def _is_available_locked(self) -> bool:
        """调用方必须持有同步锁。当前令牌存活且已同步才可查。"""
        if not self.synced:
            return False
        epoch = self.current_epoch
        return epoch is None or not epoch.dead

    async def book_view(self) -> tuple[bool, dict | None, str | None]:
        """返回 (是否可查, 盘口视图, 最近错误)。不可查时视图为 None。"""
        async with self.cond:
            if not self._is_available_locked():
                return False, None, self.last_error
            return True, self.book.view(), None

    def status_view(self) -> dict:
        s = self.settings
        return {
            "symbol": s.symbol,
            "synced": self._is_available_locked(),
            "seq": self.book.seq,
            "generation": self.generation,
            "levels": {"bids": len(self.book.bids), "asks": len(self.book.asks)},
            "last_error": self.last_error,
            "stats": dict(self.stats),
            "source": {"http": s.source_http, "ws": s.source_ws},
            "config": {
                "max_levels": s.max_levels,
                "reconnect_delay": s.reconnect_delay,
                "data_timeout": s.data_timeout,
                "snapshot_timeout": s.snapshot_timeout,
            },
        }
