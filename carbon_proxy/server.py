import asyncio
import logging
import os
import pwd
import sys
import uuid
from collections import deque
from http import HTTPStatus

import forklib
import msgpack
from aiohttp.web import (
    Application,
    Request,
    Response,
    HTTPBadRequest,
    HTTPForbidden,
    HTTPUnauthorized,
)
from aiohttp.web_urldispatcher import UrlDispatcher  # NOQA
from aiomisc.entrypoint import entrypoint
from aiomisc.periodic import PeriodicCallback
from configargparse import ArgumentParser
from setproctitle import setproctitle

from aiomisc.service import Service
from aiomisc.service.aiohttp import AIOHTTPService
from aiomisc.utils import bind_socket
from aiomisc.log import basic_config, LogFormat


log = logging.getLogger()
parser = ArgumentParser(auto_env_var_prefix="APP_")

parser.add_argument('-f', '--forks', type=int, default=4)
parser.add_argument("-u", "--user", help="Change process UID",
                    type=pwd.getpwnam)
parser.add_argument('-D', '--debug', action='store_true')


parser.add_argument('--log-level', default='info',
                    choices=('debug', 'info', 'warning', 'error', 'fatal'))

parser.add_argument('--log-format', choices=LogFormat.choices(),
                    default='color')

parser.add_argument('--pool-size', default=4, type=int)


group = parser.add_argument_group('HTTP settings')
group.add_argument('--http-address', type=str, default='0.0.0.0')
group.add_argument('--http-port', type=int, default=8081)
group.add_argument('-S', '--http-secret', type=str, required=True)

group = parser.add_argument_group('Carbon settings')
group.add_argument('-H', '--carbon-host', type=str, required=True,
                   help="TCP protocol host")

group.add_argument('-P', '--carbon-port', type=int, default=2003,
                   help="TCP protocol port")


# Should be rewrite
SECRET = str(uuid.uuid4())


async def ping(*_):
    return Response(content_type='text/plain', status=HTTPStatus.OK)


async def statistic_receiver(request: Request):

    auth = request.headers.get('Authorization')

    if not auth:
        raise HTTPUnauthorized()

    secret = auth.replace("Bearer ", '')

    if secret != SECRET:
        raise HTTPForbidden()

    payload = msgpack.unpackb(await request.read(), encoding='utf-8')

    if not isinstance(payload, list):
        raise HTTPBadRequest()

    for metric in payload:
        try:
            name, ts_value = metric
            ts, value = ts_value
            ts = float(ts)
            assert isinstance(value, (int, float, type(None)))
        except:  # noqa
            log.exception("Invalid data in %r", metric)
            raise HTTPBadRequest()

        Sender.QUEUE.append((name, value, ts))

    return Response(content_type='text/plain', status=HTTPStatus.ACCEPTED)


class Sender(Service):
    host = None  # type: str
    port = None  # type: int
    delay = 1
    bulk_size = 10000
    handle = None  # type: PeriodicCallback
    QUEUE = deque()

    async def send(self, metrics):
        reader, writer = await asyncio.open_connection(self.host, self.port)

        for name, value, timestamp in metrics:
            d = "%s %s %s\n" % (name, value, timestamp)
            writer.write(d.encode())

        await writer.drain()
        writer.close()
        reader.feed_eof()

    async def send_data(self):
        if not self.QUEUE:
            return

        metrics = []

        while self.QUEUE or len(metrics) < self.bulk_size:
            metrics.append(self.QUEUE.popleft())

        # switch context
        await asyncio.sleep(0)
        await self.send(metrics)

    async def start(self):
        log.info("Starting sender endpoint %s:%d", self.host, self.port)
        self.handle = PeriodicCallback(self.send_data)
        self.handle.start(self.delay)

    async def stop(self, *_):
        self.handle.stop()

        metrics = list(self.QUEUE)
        self.QUEUE.clear()
        await self.send(metrics)


class API(AIOHTTPService):
    debug = False
    secret = os.urandom(32)

    @staticmethod
    async def setup_routes(app: Application):
        router = app.router  # type: UrlDispatcher
        router.add_get('/ping', ping)
        router.add_post('/stat', statistic_receiver)

    async def create_application(self) -> Application:
        app = Application(debug=self.debug)
        app.on_startup.append(self.setup_routes)
        app['secret'] = self.secret
        return app


def main():
    global SECRET

    arguments = parser.parse_args()
    os.environ.clear()

    basic_config(level=arguments.log_level,
                 log_format=arguments.log_format,
                 buffered=False)

    setproctitle(os.path.basename("[Master] %s" % sys.argv[0]))

    sock = bind_socket(
        address=arguments.http_address,
        port=arguments.http_port
    )

    services = [
        API(
            debug=arguments.debug,
            secret=arguments.http_secret,
            sock=sock,
        ),
        Sender(
            host=arguments.carbon_host,
            port=arguments.carbon_port,
        )
    ]

    if arguments.user is not None:
        logging.info('Changing user to %r', arguments.user.pw_name)
        os.setgid(arguments.user.pw_gid)
        os.setuid(arguments.user.pw_uid)

    def run():
        setproctitle(os.path.basename("[Worker] %s" % sys.argv[0]))

        with entrypoint(*services,
                        pool_size=arguments.pool_size,
                        log_level=arguments.log_level,
                        log_format=arguments.log_format) as loop:
            loop.set_debug(arguments.debug)
            loop.run_forever()

    if arguments.forks:
        forklib.fork(arguments.forks, run, auto_restart=True)
    else:
        run()


if __name__ == '__main__':
    main()
