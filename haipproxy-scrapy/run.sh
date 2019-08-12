#!/bin/bash

cd /srv/haipproxy
nohup python3 crawler_booter.py --usage crawler common > /logs/crawler.log 2>&1 &
nohup python3 scheduler_booter.py --usage crawler common > /logs/crawler_scheduler.log 2>&1 &
nohup python3 crawler_booter.py --usage validator init > /logs/init_validator.log 2>&1 &
nohup python3 crawler_booter.py --usage validator https > /logs/https_validator.log 2>&1&
nohup python3 scheduler_booter.py --usage validator https > /logs/validator_scheduler.log 2>&1 &
nohup python3 squid_update.py --usage https --interval 3 > /logs/squid.log 2>&1 &
rm -rf /var/run/squid.pid

## Create missing swap directories
squid -z
## start squid
#squid -N
squid

echo "--- sleep 6m to wait haiproxy fetch proxies ---"
sleep 6m
echo "--- start douban spider now ---"

## start scrapy
cd /srv/ScrapyDouban
scrapy crawl movie_photo
