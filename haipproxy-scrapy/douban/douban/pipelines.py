#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib
import os
import douban.database as db

from douban.items import  MovieMeta, Subject, MoviePhotos
from urllib.parse import urlparse
from scrapy import Request
from scrapy.pipelines.images import ImagesPipeline
from scrapy.pipelines.files import FilesPipeline
from scrapy.utils.misc import arg_to_iter
from scrapy.utils.python import to_bytes

from twisted.internet.defer import DeferredList
import redis

REDIS_HOST = 'redis'
REDIS_PORT = 6379
REDIS_PASSWORD = '123456'
DOUBAN_DB = 1

cursor = db.connection.cursor()

class DoubanPipeline(object):
    def get_subject(self, item):
        sql = 'SELECT id FROM subjects WHERE douban_id=%s' % item['douban_id']
        cursor.execute(sql)
        return cursor.fetchone()

    def save_subject(self, item):
        keys = item.keys()
        values = tuple(item.values())
        fields = ','.join(keys)
        temp = ','.join(['%s'] * len(keys))
        sql = 'INSERT INTO subjects (%s) VALUES (%s)' % (fields, temp)
        cursor.execute(sql, values)
        return db.connection.commit()

    def get_movie_meta(self, item):
        sql = 'SELECT id FROM movies WHERE douban_id=%s' % item['douban_id']
        cursor.execute(sql)
        return cursor.fetchone()

    def save_movie_meta(self, item):
        keys = item.keys()
        values = tuple(item.values())
        fields = ','.join(keys)
        temp = ','.join(['%s'] * len(keys))
        sql = 'INSERT INTO movies (%s) VALUES (%s)' % (fields, temp)
        cursor.execute(sql, tuple(i.strip() for i in values))
        return db.connection.commit()

    def update_movie_meta(self, item):
        douban_id = item.pop('douban_id')
        keys = item.keys()
        values = tuple(item.values()) + (douban_id,)
        #values.append(douban_id)
        fields = ['%s=' % i + '%s' for i in keys]
        sql = 'UPDATE movies SET %s WHERE douban_id=%s' % (','.join(fields), '%s')
        cursor.execute(sql, tuple(i.strip() for i in values))
        return db.connection.commit()

    def process_item(self, item, spider):
        if isinstance(item, Subject):
            '''
            subject
            '''
            exist = self.get_subject(item)
            if not exist:
                self.save_subject(item)
        elif isinstance(item, MovieMeta):
            '''
            meta
            '''
            exist = self.get_movie_meta(item)
            if not exist:
                try:
                    self.save_movie_meta(item)
                except Exception as e:
                    print(item)
                    print(e)
            else:
                self.update_movie_meta(item)
        return item

class CoverPipeline(ImagesPipeline):
    def process_item(self, item, spider):
        if 'meta' not in spider.name:
            return item
        info = self.spiderinfo
        requests = arg_to_iter(self.get_media_requests(item, info))
        dlist = [self._process_request(r, info) for r in requests]
        dfd = DeferredList(dlist, consumeErrors=1)
        return dfd.addCallback(self.item_completed, item, info)

    def file_path(self, request, response=None, info=None):
        # start of deprecation warning block (can be removed in the future)
        def _warn():
            from scrapy.exceptions import ScrapyDeprecationWarning
            import warnings
            warnings.warn('ImagesPipeline.image_key(url) and file_key(url) methods are deprecated, '
                          'please use file_path(request, response=None, info=None) instead',
                          category=ScrapyDeprecationWarning, stacklevel=1)

        # check if called from image_key or file_key with url as first argument
        if not isinstance(request, Request):
            _warn()
            url = request
        else:
            url = request.url

        # detect if file_key() or image_key() methods have been overridden
        if not hasattr(self.file_key, '_base'):
            _warn()
            return self.file_key(url)
        elif not hasattr(self.image_key, '_base'):
            _warn()
            return self.image_key(url)
        # end of deprecation warning block

        image_guid = hashlib.sha1(to_bytes(url)).hexdigest()
        return '%s%s/%s%s/%s.jpg' % (image_guid[9], image_guid[19], image_guid[29], image_guid[39], image_guid)

    def get_media_requests(self, item, info):
        if item['cover']:
            return Request(item['cover'])

    def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        if image_paths:
            item['cover'] = image_paths[0]
        else:
            item['cover'] = ''
        return item

class PhotoPipeline(FilesPipeline):

    r = redis.StrictRedis(REDIS_HOST, REDIS_PORT, DOUBAN_DB, REDIS_PASSWORD)

    def get_image_id(self, imageBaseName):
        return imageBaseName[1:imageBaseName.rindex('.')]

    def get_media_requests(self, item, info):
        for image_url in item['image_urls']:
            baseName = os.path.basename(urlparse(image_url).path)
            image_id = self.get_image_id(baseName)
            PhotoPipeline.r.set(image_id, item['douban_id'])
            sql = "INSERT INTO movie_image (douban_id, image_id, url, status) VALUES (%s, %s, '%s', 0)" % (item['douban_id'], image_id, image_url)
            cursor.execute(sql)
            db.connection.commit()

        for image_url in item['image_urls']:
            yield Request(image_url)

    def file_path(self, request, response=None, info=None):
        url = request.url
        baseName = os.path.basename(urlparse(url).path)

        dbID = '000'
        image_id = self.get_image_id(baseName)
        if PhotoPipeline.r.exists(image_id):
            dbID = bytes.decode(PhotoPipeline.r.get(image_id))
            
        return '%s/%s' % (dbID, baseName)

    def item_completed(self, results, item, info):
        paths = [x['path'] for ok, x in results if ok]
        if paths:
            item['image_paths'] = paths
            
            for path in paths:
                baseName = os.path.basename(path)
                image_id = self.get_image_id(baseName)

                sql = 'UPDATE movie_image SET status = 1 WHERE douban_id = %s AND image_id = %s' % (item['douban_id'], image_id)
                cursor.execute(sql)
                db.connection.commit()

                PhotoPipeline.r.delete(image_id)

            sql2 = 'UPDATE movie_basic SET image_download = %d WHERE subject_id = %s' % (len(paths), item['douban_id'])
            cursor.execute(sql2)
            db.connection.commit()

        return item
