#!/usr/bin/env python
# -*- coding: utf-8 -*-


import string
import random
import douban.util as util
import douban.database as db
import douban.validator as validator

from scrapy import Request, Spider
from scrapy.exceptions import CloseSpider
from douban.items import MoviePhotos


cursor = db.connection.cursor()


class MoviePhoteSpider(Spider):
    name = 'movie_photo'
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36'
    allowed_domains = ["movie.douban.com"]
    sql = 'SELECT subject_id FROM movie_basic where subject_id > 0 and image_download = 0'
    cursor.execute(sql)
    movies = cursor.fetchall()
    start_urls = (
        'https://movie.douban.com/subject/%s/photos?type=S' % i['subject_id'] for i in movies
    )

    def start_requests(self):
        for url in self.start_urls:
            bid = ''.join(random.choice(string.ascii_letters + string.digits) for x in range(11))
            cookies = {
                'bid': bid,
                'dont_redirect': True,
                'handle_httpstatus_list': [302],
            }
            yield Request(url, cookies=cookies)

    def get_douban_id(self, meta, response):
        meta['douban_id'] = response.url[33:-14]
        return meta

    def get_image_urls(self, meta, response):
        regx = '//div[@class="cover"]/a/img/@src'
        image_urls = response.xpath(regx).extract()
        ##只需要前10个
        urls = image_urls[:10]
        ##换成大图链接
        urls = [url.replace('/m/', '/l/').replace('.jpg', '.webp') for url in urls]
        meta['image_urls'] = urls
        return meta

    def parse(self, response):
        if  403 == response.status:
            print(response.url)
            raise CloseSpider('403')
        else:
            meta = MoviePhotos()
            self.get_douban_id(meta, response)
            self.get_image_urls(meta, response)
            
            return meta
