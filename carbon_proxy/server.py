import asyncio
import logging
import os
import pwd
import sys
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
from configargparse import ArgumentParser
from setproctitle import setproctitle

from aiomisc.service.periodic import PeriodicService
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

group = parser.add_argument_group('Sender settings')
parser.add_argument('--sender-interval', default=1, type=int)


async def ping(*_):
    return Response(content_type='text/plain', status=HTTPStatus.OK)


async def statistic_receiver(request: Request):

    auth = request.headers.get('Authorization')

    if not auth:
        raise HTTPUnauthorized()

    secret = auth.replace("Bearer ", '')

    if request.app['secret'] != secret:
        raise HTTPForbidden()

    payload = msgpack.unpackb(await request.read(), raw=False)

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


class API(AIOHTTPService):
    __required__ = ('secret',)

    secret: str = None

    @staticmethod
    async def setup_routes(app: Application):
        router = app.router  # type: UrlDispatcher
        router.add_get('/ping', ping)
        router.add_post('/stat', statistic_receiver)

    async def create_application(self) -> Application:
        app = Application()
        app.on_startup.append(self.setup_routes)
        app['secret'] = self.secret
        return app


class Sender(PeriodicService):
    host: str = None
    port: int = None
    interval: int = 1
    bulk_size: int = 10000
    QUEUE: deque = deque()

    async def send(self, metrics):
        reader, writer = await asyncio.open_connection(self.host, self.port)

        for name, value, timestamp in metrics:
            d = "%s %s %s\n" % (name, value, timestamp)
            writer.write(d.encode())

        await writer.drain()
        writer.close()
        reader.feed_eof()

    async def callback(self):
        if not self.QUEUE:
            return

        metrics = []

        while self.QUEUE or len(metrics) < self.bulk_size:
            try:
                metrics.append(self.QUEUE.popleft())
            except IndexError:
                break

        # switch context
        await asyncio.sleep(0)
        await self.send(metrics)

    async def stop(self, *args, **kwargs):
        await super().stop(*args, **kwargs)

        metrics = list(self.QUEUE)
        self.QUEUE.clear()
        await self.send(metrics)


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
            secret=arguments.http_secret,
            sock=sock,
        ),
        Sender(
            host=arguments.carbon_host,
            port=arguments.carbon_port,
            interval=arguments.sender_interval,
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
