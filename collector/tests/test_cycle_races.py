"""状态机竞态测试：覆盖两个失步交接缺陷。

- 缺号/断流一经发现，/book 立即 503，不存在“清理未结束仍返回 200”的窗口；
- 快照已返回、等待状态锁期间连接断开，锁释放后该快照必须被拒绝，
  已断连接的快照不得把盘口重新置为可查；
- 只有新连接的快照与连续增量完整衔接后才恢复可查。

全部使用内存假 WS / 假 HTTP，不依赖真实网络。
"""
import asyncio
import json

import pytest

import app.sync_manager as sm_mod
from app.config import Settings
from app.orderbook import StaleSnapshotError
from app.sync_manager import ConnectionEpoch, SyncManager

SYMBOL = "CU2601"


# ---------- 测试夹具 ----------

def inc(seq: int) -> dict:
    return {
        "type": "increment",
        "symbol": SYMBOL,
        "seq": seq,
        "changes": [{"side": "bid", "price": 69000 + seq, "qty": 2}],
    }


def snap(seq: int) -> dict:
    return {
        "type": "snapshot",
        "symbol": SYMBOL,
        "seq": seq,
        "bids": [[69990, 5]],
        "asks": [[70010, 7]],
    }


class FakeWS:
    """假 WebSocket。

    帧耗尽后阻塞；将 fail 置位后，下一次 recv() 直接抛断开，
    精确模拟真实 socket：recv() 抛错即连接死亡的同步时刻，
    采集器在该异常分支里就地置令牌 dead，控制权才回到事件循环。

    gate 置位时每交付一帧前先等待 gate，供测试控制帧的投递节奏。
    """

    def __init__(self, frames: list, gate: asyncio.Event | None = None) -> None:
        self.frames = list(frames)
        self.fail = False
        self.gate = gate
        self.cancelled = False
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.cancelled = True

    async def recv(self):
        await asyncio.sleep(0)
        if self.fail:
            raise ConnectionError("stream closed by peer")
        if self.frames:
            if self.gate is not None:
                await self.gate.wait()
            frame = self.frames.pop(0)
            if self.gate is not None:
                # 单帧闸门：每交付一帧即关闭，测试需再次 set 才放下一帧
                self.gate.clear()
            return frame if isinstance(frame, str) else json.dumps(frame)
        while not self.fail:  # 阻塞等待外部故障注入
            await asyncio.sleep(0.005)
        raise ConnectionError("stream closed by peer")


class FakeResp:
    def raise_for_status(self) -> None:
        pass

    def __init__(self, data: dict) -> None:
        self._data = data

    def json(self) -> dict:
        return self._data


class DelayedSnapshot:
    def __init__(self, event: asyncio.Event, data: dict) -> None:
        self.event = event
        self.data = data


class FakeHttp:
    def __init__(self, items: list) -> None:
        self.items = list(items)

    async def get(self, path, params=None):
        item = self.items.pop(0)
        if isinstance(item, DelayedSnapshot):
            await item.event.wait()
            return FakeResp(item.data)
        return FakeResp(item)

    async def aclose(self) -> None:
        pass


def make_manager(gen: int = 1) -> SyncManager:
    mgr = SyncManager(
        Settings(
            symbol=SYMBOL,
            source_http="http://x",
            source_ws="ws://x",
            reconnect_delay=0.01,
            data_timeout=5.0,
        )
    )
    mgr.generation = gen
    return mgr


async def wait_for(predicate, timeout: float = 2.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition not met before timeout")


def patch_connect(monkeypatch, sockets: list[FakeWS]):
    it = iter(sockets)

    def _connect(*args, **kwargs):
        try:
            return next(it)
        except StopIteration:  # 测试夹具耗尽：模拟行情源不可达
            raise ConnectionError("no more fake sockets")

    monkeypatch.setattr(sm_mod.websockets, "connect", _connect)


# ---------- 正常衔接 ----------

def test_cycle_happy_path_book_available_then_closed(monkeypatch):
    async def scenario():
        mgr = make_manager()
        ws = FakeWS([inc(101)])
        patch_connect(monkeypatch, [ws])
        mgr._http = FakeHttp([snap(100)])
        epoch = ConnectionEpoch(1)

        task = asyncio.create_task(mgr._cycle(epoch))
        await wait_for(lambda: mgr.synced)

        ok, view, err = await mgr.book_view()
        assert ok and err is None
        assert view["seq"] == 101

        # 断流后立即不可查，盘口不再对外可用
        ws.fail = True
        with pytest.raises(ConnectionError):
            await task
        ok2, _, _ = await mgr.book_view()
        assert ok2 is False
        assert epoch.dead is True
        # inc(101) 在快照在途期间到达，经缓冲衔接计入 buffered_applied
        assert mgr.book.seq == 101
        assert (mgr.stats["increments_applied"] + mgr.stats["buffered_applied"]) == 1
        assert mgr.stats["snapshots_applied"] == 1

    asyncio.run(scenario())


# ---------- 缺陷二：快照在途时连接断开 ----------

def test_close_before_snapshot_arrives_rejects_cycle(monkeypatch):
    async def scenario():
        mgr = make_manager()
        snap_event = asyncio.Event()
        ws = FakeWS([{"type": "heartbeat"}])  # 消费一帧后阻塞，随后可被关闭
        patch_connect(monkeypatch, [ws])
        mgr._http = FakeHttp([DelayedSnapshot(snap_event, snap(100))])
        epoch = ConnectionEpoch(1)

        task = asyncio.create_task(mgr._cycle(epoch))
        await asyncio.sleep(0.05)
        assert epoch.dead is False

        # 连接在快照返回前断开：recv() 同步置令牌 dead，本轮立即作废
        ws.fail = True
        await wait_for(lambda: epoch.dead)

        # 让迟到的快照响应返回；本轮必须作废，快照不得安装
        snap_event.set()
        with pytest.raises(ConnectionError):
            await task

        ok, _, _ = await mgr.book_view()
        assert ok is False
        assert mgr.book.seq is None
        assert mgr.stats["snapshots_applied"] == 0

    asyncio.run(scenario())


# ---------- 缺陷二（精确复现）：快照已返回，等锁期间连接断开 ----------

def test_close_while_install_waits_on_lock_rejects_snapshot(monkeypatch):
    async def scenario():
        mgr = make_manager()
        ws = FakeWS([{"type": "heartbeat"}])  # 消费心跳后下一次 recv 阻塞/抛错
        patch_connect(monkeypatch, [ws])
        mgr._http = FakeHttp([snap(100)])
        epoch = ConnectionEpoch(1)

        # 先发布本周期令牌（模拟 _run 的发布动作）
        async with mgr.lock:
            mgr.current_epoch = epoch
        # 查询侧/其它持有者再占住状态锁，迫使快照安装排队等待
        await mgr.lock.acquire()
        task = asyncio.create_task(mgr._cycle(epoch))

        # 等到有协程排队等锁：守卫停在条件队列、安装协程随后停在底层锁队列
        def has_waiter() -> bool:
            cond_waiters = getattr(mgr.cond, "_waiters", None)
            lock_waiters = getattr(mgr.cond._lock, "_waiters", None)
            return bool(cond_waiters) or bool(lock_waiters)

        await wait_for(has_waiter)
        # 再等到快照安装协程本身已停在底层锁队列上
        await wait_for(lambda: bool(getattr(mgr.cond._lock, "_waiters", None)))
        assert mgr.synced is False and mgr.book.seq is None

        # 连接在快照安装拿到锁之前已断。recv() 失败回调就地把 epoch 标 dead
        # （无锁同步赋值）；守卫再经 Condition 队列优先拿锁置 synced=False。
        # 安装协程即使先被唤醒，也会在第三道闸看到 epoch.dead 而拒绝落簿。
        ws.fail = True
        mgr.lock.release()

        with pytest.raises(ConnectionError):
            await task

        ok, _, _ = await mgr.book_view()
        assert ok is False
        assert mgr.synced is False
        assert mgr.book.seq is None
        assert mgr.stats["snapshots_applied"] == 0
        assert epoch.dead is True

    asyncio.run(scenario())


# ---------- 缺陷一 + 完整自愈：缺号即 503，新连接衔接后恢复 ----------

def test_gap_immediately_unsynced_and_new_connection_recovers(monkeypatch):
    async def scenario():
        mgr = make_manager()
        # 用投递闸门控制节奏：先让 inc(101) 完成首轮同步，再放行缺号帧
        gate1 = asyncio.Event()
        gate1.set()  # 首帧（inc 101）立即可取
        ws1 = FakeWS([inc(101), inc(103)], gate=gate1)  # 102 缺失 -> 缺号
        ws2 = FakeWS([inc(201)])
        patch_connect(monkeypatch, [ws1, ws2])
        mgr._http = FakeHttp([snap(100), snap(200)])

        run_task = asyncio.create_task(mgr._run())
        try:
            # 首轮同步：快照 seq=100 + 增量衔接到 seq=101
            await wait_for(lambda: mgr.synced and mgr.book.seq == 101)
            ok, view, _ = await mgr.book_view()
            assert ok and view["seq"] == 101, view

            # 监视器：缺号被发现（gaps 计数）之后、新快照（seq 200）安装之前，
            # /book 一次都不允许返回可查 —— 修复前会在拆除窗口返回 200。
            violations: list[int] = []
            monitoring = True

            async def monitor():
                while monitoring:
                    if mgr.stats["gaps"] > 0 and (mgr.book.seq is None or mgr.book.seq < 200):
                        ok, view, _ = await mgr.book_view()
                        if ok:
                            violations.append(view["seq"])
                    await asyncio.sleep(0)

            mon_task = asyncio.create_task(monitor())

            # 放行缺号帧 inc(103)（ws1 在取走首帧后会再次等 gate）
            gate1.set()
            await wait_for(lambda: mgr.stats["gaps"] > 0)

            # 缺号发现的同一时刻即失步，无需等待清理
            ok, _, _ = await mgr.book_view()
            assert ok is False
            assert mgr.synced is False

            # 新连接快照 + 连续增量衔接完成后才恢复
            await wait_for(lambda: mgr.synced and mgr.book.seq == 201, timeout=5.0)
            ok, view, _ = await mgr.book_view()
            assert ok and view["seq"] == 201, view

            monitoring = False
            await mon_task
            assert violations == []
            assert mgr.stats["resyncs"] >= 1
        finally:
            ws1.fail = True
            ws2.fail = True
            run_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run_task

    asyncio.run(scenario())


# ---------- 世代防护：迟到快照在安装处仍被拒绝 ----------

def test_stale_snapshot_never_reopens_book(monkeypatch):
    async def scenario():
        mgr = make_manager(gen=2)
        live = ConnectionEpoch(2)
        async with mgr.lock:
            mgr.synced = True
            mgr.current_epoch = live
        stale_epoch = ConnectionEpoch(1)
        with pytest.raises(StaleSnapshotError):
            await mgr._install_snapshot(snap(50), [], epoch=stale_epoch)
        ok, view, _ = await mgr.book_view()
        assert ok is True  # 新世代盘口不受影响
        assert view["seq"] is None or view["seq"] != 50

    asyncio.run(scenario())


# ---------- 死连接快照（同世代但令牌 dead）不得恢复可查 ----------

def test_dead_epoch_snapshot_refused_even_same_generation(monkeypatch):
    async def scenario():
        mgr = make_manager()
        epoch = ConnectionEpoch(1)
        async with mgr.lock:
            mgr.current_epoch = epoch
        epoch.mark_dead()  # 连接已断，但世代号尚未递增
        with pytest.raises(ConnectionError):
            await mgr._install_snapshot(snap(100), [], epoch=epoch)
        ok, _, _ = await mgr.book_view()
        assert ok is False
        assert mgr.stats["snapshots_applied"] == 0

    asyncio.run(scenario())
