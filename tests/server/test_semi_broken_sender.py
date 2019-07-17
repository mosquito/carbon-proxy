import asyncio

import pytest


@pytest.fixture
def routes(routes, aiomisc_unused_port_factory):
    port = aiomisc_unused_port_factory()
    return f'egg=localhost:{port},' + routes


async def test_dont_stop_sending_metrics_to_alive_upstreams(
        http_session_auth, stat_url, tcp_server, foo_bar_tcp_server, pack
):
    metrics = [
        ['egg.first', [1548934965.0, 404]],
        ['foo.bar.1', [1548934966.0, 42]],
        ['test.spam', [1548934967.0, 33]],
        ['foo.bar.spam', [1548934968.0, 55]],
        ['egg.last', [1548934969.0, 101]],
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
