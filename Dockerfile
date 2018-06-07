FROM mosquito/ubuntu-python36 as builder

RUN apt-get install -y \
    build-essential \
    make \
    git-core \
    libfreetype6-dev \
    libgif-dev \
    libjpeg-dev \
    liblcms2-dev \
    libopenjp2-7-dev \
    libpng-dev \
    libtiff5-dev \
    libwebp-dev \
    python3-pip \
    python3-wheel \
    virtualenv \
    tcl-dev \
    tk-dev \
    zlib1g-dev

RUN virtualenv -p python3 /usr/share/python3/app

ADD requirements.txt /tmp/
RUN /usr/share/python3/app/bin/pip install -Ur /tmp/requirements.txt

ADD dist/ /tmp/app/
RUN /usr/share/python3/app/bin/pip install /tmp/app/*


########################################################################
FROM mosquito/ubuntu-python36:latest

COPY --from=builder /usr/share/python3/app /usr/share/python3/app
RUN ln -snf /usr/share/python3/app/bin/carbon-proxy /usr/bin/
RUN ln -snf /usr/share/python3/app/bin/carbon-proxy-server /usr/bin/

CMD /usr/bin/carbon-proxy
