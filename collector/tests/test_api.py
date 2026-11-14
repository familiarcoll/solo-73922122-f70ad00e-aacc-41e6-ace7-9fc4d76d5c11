"""HTTP 层契约测试：/book 的 200/503 语义与 /status 持续可用。

不进入 lifespan（不启动后台同步任务），直接操纵 SyncManager 的状态验证端点。
"""
import pytest
from fastapi.testclient import TestClient

import app.main as main_mod
from app.sync_manager import ConnectionEpoch


@pytest.fixture()
def client():
    # 不使用 with：Starlette 仅在 __enter__ 时跑 lifespan，
    # 这里不启动后台同步任务，避免连接真实行情源、污染共享 manager。
    c = TestClient(main_mod.app, raise_server_exceptions=True)
    mgr = main_mod.manager
    mgr.synced = False
    mgr.current_epoch = None
    mgr.last_error = None
    yield c


def test_book_503_when_unsynced(client):
    mgr = main_mod.manager
    mgr.synced = False
    mgr.current_epoch = None
    r = client.get("/book")
    assert r.status_code == 503
    body = r.json()
    assert body["error"] == "book_not_synchronized"
    assert body["synced"] is False


def test_book_503_when_epoch_dead_even_if_synced_flag_true(client):
    """synced 标志为 True 但连接令牌已死 -> 仍须 503（旧盘口不得重新可查）。"""
    mgr = main_mod.manager
    epoch = ConnectionEpoch(99)
    mgr.generation = 99
    mgr.synced = True
    mgr.current_epoch = epoch
    try:
        # 存活时可查
        assert client.get("/book").status_code == 200
        # 连接死亡，即便 synced 标志尚未被清理，查询也立即 503
        epoch.mark_dead()
        r = client.get("/book")
        assert r.status_code == 503
        assert r.json()["synced"] is False
    finally:
        mgr.synced = False
        mgr.current_epoch = None


def test_status_always_available_and_reflects_state(client):
    mgr = main_mod.manager
    mgr.synced = False
    mgr.current_epoch = None
    r = client.get("/status")
    assert r.status_code == 200
    body = r.json()
    assert body["synced"] is False
    assert "generation" in body and "stats" in body and "seq" in body


def test_index_and_openapi_docs(client):
    assert client.get("/").status_code == 200
    assert client.get("/docs").status_code == 200
    assert client.get("/openapi.json").status_code == 200
