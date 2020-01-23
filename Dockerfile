FROM snakepacker/python:all as builder

RUN python3.7 -m venv /usr/share/python3/app

RUN /usr/share/python3/app/bin/pip install -U pip

ADD requirements.txt /tmp/
RUN /usr/share/python3/app/bin/pip install -Ur /tmp/requirements.txt

ADD dist/ /tmp/app/
RUN /usr/share/python3/app/bin/pip install /tmp/app/*

########################################################################
FROM snakepacker/python:3.7 as jaeger-proxy-server

COPY --from=builder /usr/share/python3/app /usr/share/python3/app
RUN ln -snf /usr/share/python3/app/bin/jaeger-proxy-server /usr/bin/

CMD ["/usr/bin/jaeger-proxy-server"]
