FROM snakepacker/python:all as builder

RUN virtualenv -p python3 /usr/share/python3/app

ADD requirements.txt /tmp/
RUN /usr/share/python3/app/bin/pip install -Ur /tmp/requirements.txt

ADD dist/ /tmp/app/
RUN /usr/share/python3/app/bin/pip install /tmp/app/*


########################################################################
FROM snakepacker/python:3.7

COPY --from=builder /usr/share/python3/app /usr/share/python3/app

RUN ln -snf /usr/share/python3/app/bin/carbon-proxy \
            /usr/share/python3/app/bin/carbon-proxy-server \
            /usr/bin/

CMD ["/usr/bin/carbon-proxy"]
