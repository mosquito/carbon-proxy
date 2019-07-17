import msgpack


def pack(data):
    return msgpack.packb(data, use_bin_type=True)


async def test_no_auth(http_session, api_url):
    resp = await http_session.post(api_url / 'stat')
    assert resp.status == 401


async def test_wrong_auth(http_session, api_url):
    resp = await http_session.post(
        api_url / 'stat',
        headers={'Authorization': 'wrong'},
    )
    assert resp.status == 403


async def test_correct_auth(http_session_auth, api_url):
    resp = await http_session_auth.post(api_url / 'stat', data=pack([]))
    assert resp.status == 202
