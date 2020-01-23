from aiohttp import BasicAuth
from async_timeout import timeout


async def test_no_auth(http_session, stat_url):
    resp = await http_session.post(stat_url)
    assert resp.status == 401


async def test_wrong_auth(http_session, stat_url):
    resp = await http_session.post(
        stat_url, headers={"Authorization": "wrong"}
    )
    assert resp.status == 403


async def test_wrong_pass(http_session, stat_url):
    resp = await http_session.post(
        stat_url,
        auth=BasicAuth(login="edadeal", password="wrong"),
    )
    assert resp.status == 403


async def test_correct_auth(http_session_auth, stat_url, pack):
    resp = await http_session_auth.post(stat_url, data=pack([]))
    assert resp.status == 202


async def test_accepts_metrics(
    http_session_auth, stat_url, jaeger_mock_server, pack
):
    metrics = pack([
        ["test.spam", [1548934966.0, 42]],
        ["test.spam", [1548934967.0, 33]],
        ["test.spam", [1548934968.0, 55]],
    ])

    resp = await http_session_auth.post(stat_url, data=metrics)
    assert resp.status == 202

    with timeout(1):
        data = await jaeger_mock_server.wait()

    assert data == metrics
