import asyncio


async def test_routing(http_session_auth, stat_url, tcp_server,
                       foo_bar_tcp_server, pack):
    metrics = [
        ['foo.bar.1', [1548934966.0, 42]],
        ['test.spam', [1548934967.0, 33]],
        ['foo.bar.spam', [1548934968.0, 55]],
    ]

    resp = await http_session_auth.post(stat_url, data=pack(metrics))
    assert resp.status == 202

    await asyncio.gather(
        tcp_server.wait_data(), foo_bar_tcp_server.wait_data(),
    )

    received_root_metrics = tcp_server.data.decode().strip().split('\n')

    assert received_root_metrics == [
        'test.spam 33 1548934967.0',
    ]

    received_foo_bar_metrics = (
        foo_bar_tcp_server.data.decode().strip().split('\n')
    )

    assert received_foo_bar_metrics == [
        'foo.bar.1 42 1548934966.0',
        'foo.bar.spam 55 1548934968.0',
    ]
