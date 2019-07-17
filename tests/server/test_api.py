import msgpack


def pack(data):
    return msgpack.packb(data, use_bin_type=True)


async def test_no_auth(http_session, stat_url):
    resp = await http_session.post(stat_url)
    assert resp.status == 401


async def test_wrong_auth(http_session, stat_url):
    resp = await http_session.post(stat_url, headers={'Authorization': 'wrong'})
    assert resp.status == 403


async def test_correct_auth(http_session_auth, stat_url):
    resp = await http_session_auth.post(stat_url, data=pack([]))
    assert resp.status == 202


async def test_accepts_metrics(http_session_auth, stat_url, tcp_server):
    metrics = [
        ['foo.bar.spam', [1548934966.0, 42]],
        ['foo.bar.spam', [1548934967.0, 33]],
        ['foo.bar.spam', [1548934968.0, 55]],
    ]

    resp = await http_session_auth.post(stat_url, data=pack(metrics))
    assert resp.status == 202

    await tcp_server.wait_data()

    received_metrics = tcp_server.data.decode().strip().split('\n')

    assert received_metrics == [
        'foo.bar.spam 42 1548934966.0',
        'foo.bar.spam 33 1548934967.0',
        'foo.bar.spam 55 1548934968.0',
    ]
