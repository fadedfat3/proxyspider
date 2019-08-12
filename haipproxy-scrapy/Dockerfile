FROM alpine:3.8

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
ENV ISDOCKER 1

RUN echo -e 'http://mirrors.aliyun.com/alpine/v3.8/main/\nhttp://mirrors.aliyun.com/alpine/v3.8/community/' > /etc/apk/repositories
RUN apk upgrade --no-cache \
  && apk add --no-cache \
  squid libxml2-dev libxml2 \
  libxslt-dev libxslt \
  gcc musl musl-dev libffi-dev \
  jpeg-dev curl curl-dev make \
  python3 python3-dev tzdata \
  && cp /usr/share/zoneinfo/Asia/Chongqing /etc/localtime \
  && echo 'Asia/Chongqing' > /etc/timezone \
  && apk del tzdata \
  && python3 -m pip install -i https://mirrors.aliyun.com/pypi/simple/ --upgrade pip \
  && python3 -m pip install -i https://mirrors.aliyun.com/pypi/simple/ scrapy pymysql pillow arrow \
  raven redis requests schedule Twisted Flask scrapy-splash prometheus-client click \
  && rm -rf /var/cache/* \
  && rm -rf /root/.cache/*

RUN sed -i 's/http_access deny all/http_access allow all/g' /etc/squid/squid.conf && cp /etc/squid/squid.conf /etc/squid/squid.conf.backup

COPY ./run.sh /run.sh
