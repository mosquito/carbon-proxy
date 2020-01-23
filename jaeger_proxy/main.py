import asyncio
import logging
import os
import pwd
import sys
from asyncio import gather
from collections import deque
from http import HTTPStatus
from typing import NamedTuple

import forklib
from aiohttp import BasicAuth, ClientSession, hdrs
from aiohttp.web import (
    Application,
    HTTPForbidden,
    HTTPUnauthorized,
    Request,
    Response,
)
from aiomisc.entrypoint import entrypoint
from aiomisc.log import LogFormat, basic_config
from aiomisc.service.aiohttp import AIOHTTPService
from aiomisc.service.periodic import PeriodicService
from aiomisc.utils import bind_socket
from configargparse import ArgumentParser
from setproctitle import setproctitle
from yarl import URL

log = logging.getLogger()

parser = ArgumentParser(auto_env_var_prefix="APP_")

parser.add_argument("-f", "--forks", type=int, default=4)
parser.add_argument(
    "-u", "--user", help="Change process UID", type=pwd.getpwnam
)
parser.add_argument("-D", "--debug", action="store_true")

parser.add_argument(
    "--log-level",
    default="info",
    choices=("debug", "info", "warning", "error", "fatal"),
)

parser.add_argument(
    "--log-format", choices=LogFormat.choices(), default="color"
)

parser.add_argument("--pool-size", default=4, type=int)

group = parser.add_argument_group("HTTP settings")
group.add_argument("--http-address", type=str, default="0.0.0.0")
group.add_argument("--http-port", type=int, default=8080)
group.add_argument("--http-password", type=str, required=True)
group.add_argument("--http-login", type=str, default="edadeal")

group = parser.add_argument_group("Jaeger settings")
group.add_argument("--jaeger-route", type=URL, required=True)

group = parser.add_argument_group("Sender settings")
parser.add_argument("--sender-interval", default=1, type=float)


async def ping(*_):
    return Response(content_type="text/plain", status=HTTPStatus.OK)


BYPASS_HEADERS = [
    hdrs.ACCEPT_ENCODING,
    hdrs.CONTENT_LENGTH,
    hdrs.CONTENT_TYPE,
    hdrs.USER_AGENT,
]


async def statistic_receiver(request: Request):
    auth = request.headers.get("Authorization")

    if not auth:
        raise HTTPUnauthorized
    try:
        basic = BasicAuth.decode(auth)
    except ValueError:
        log.exception("Failed to parse basic auth")
        raise HTTPForbidden

    if request.app["password"] != basic.password:
        raise HTTPForbidden

    if request.app["login"] != basic.login:
        raise HTTPForbidden

    data = await request.read()

    bypass_headers = {
        header: request.headers.get(header)
        for header in BYPASS_HEADERS
        if header
    }
    Sender.QUEUE.append((data, bypass_headers))

    return Response(content_type="text/plain", status=HTTPStatus.ACCEPTED)


class API(AIOHTTPService):
    __required__ = "password", "login"

    password: str
    login: str

    @staticmethod
    async def setup_routes(app: Application):
        router = app.router  # type: UrlDispatcher
        router.add_get("/ping", ping)
        router.add_post("/api/traces", statistic_receiver)

    async def create_application(self) -> Application:
        app = Application()
        app.on_startup.append(self.setup_routes)
        app["password"] = self.password
        app["login"] = self.login
        return app


class Connection(NamedTuple):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    def write(self, data):
        return self.writer.write(data)


class Sender(PeriodicService):
    __required__ = (
        "jaeger_route",
        "interval",
    )

    jaeger_route: URL
    interval: float

    bulk_size: int = 10000
    QUEUE: deque = deque()
    session: ClientSession = None

    async def callback(self):
        if not self.QUEUE:
            return

        metrics = []

        while self.QUEUE or len(metrics) < self.bulk_size:
            try:
                metrics.append(self.QUEUE.popleft())
            except IndexError:
                break

        await self.send(metrics)

    async def stop(self, *args, **kwargs):
        await super().stop(*args, **kwargs)

        metrics = list(self.QUEUE)
        self.QUEUE.clear()
        await self.send(metrics)

    async def send(self, metrics):
        if not metrics:
            return
        async with ClientSession() as conn:
            print(self.jaeger_route, "metrics", metrics)
            await gather(
                *[
                    conn.post(self.jaeger_route, data=data, headers=headers)
                    for data, headers in metrics
                ]
            )


def main():
    arguments = parser.parse_args()
    os.environ.clear()

    basic_config(
        level=arguments.log_level,
        log_format=arguments.log_format,
        buffered=False,
    )

    setproctitle(os.path.basename("[Master] %s" % sys.argv[0]))

    sock = bind_socket(
        address=arguments.http_address, port=arguments.http_port
    )

    services = [
        API(
            sock=sock,
            password=arguments.http_password,
            login=arguments.http_login,
        ),
        Sender(
            jaeger_route=arguments.jaeger_route,
            interval=arguments.sender_interval,
        ),
    ]

    if arguments.user is not None:
        logging.info("Changing user to %r", arguments.user.pw_name)
        os.setgid(arguments.user.pw_gid)
        os.setuid(arguments.user.pw_uid)

    def run():
        setproctitle(os.path.basename("[Worker] %s" % sys.argv[0]))

        with entrypoint(
            *services,
            pool_size=arguments.pool_size,
            log_level=arguments.log_level,
            log_format=arguments.log_format,
            debug=arguments.debug
        ) as loop:
            loop.run_forever()

    if arguments.forks:
        forklib.fork(arguments.forks, run, auto_restart=True)
    else:
        run()


if __name__ == "__main__":
    main()
