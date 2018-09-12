FROM snakepacker/python:all as builder

RUN virtualenv -p python3.7 /usr/share/python3/app

ADD requirements.txt /tmp/
RUN /usr/share/python3/app/bin/pip install -Ur /tmp/requirements.txt

ADD dist/ /tmp/app/
RUN /usr/share/python3/app/bin/pip install /tmp/app/*

########################################################################
FROM snakepacker/python:3.7 as base

COPY --from=builder /usr/share/python3/app /usr/share/python3/app
RUN ln -snf /usr/share/python3/app/bin/carbon-proxy /usr/bin/
RUN ln -snf /usr/share/python3/app/bin/carbon-proxy-server /usr/bin/

########################################################################
FROM base as carbon-proxy

RUN mkdir -p /var/lib/carbon-proxy
ENV TMPDIR /var/lib/carbon-proxy
VOLUME /var/lib/carbon-proxy

CMD ["/usr/bin/carbon-proxy", "--storage=/var/lib/carbon-proxy"]

########################################################################
FROM base as carbon-proxy-server
CMD ["/usr/bin/carbon-proxy-server"]
