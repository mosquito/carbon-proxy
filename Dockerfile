FROM python:3.6-alpine

RUN apk --no-cache add gzip tar gcc musl-dev make
RUN pip3.6 install uvloop

ADD requirements.txt /tmp/
RUN pip3.6 install -r /tmp/requirements.txt && rm -fr /tmp/* /var/tmp/*

ENV VERSION 0.1.1

ADD dist/carbon-proxy-${VERSION}.tar.gz /tmp/

RUN pip3.6 install /tmp/* && \
    rm -fr /tmp/*
