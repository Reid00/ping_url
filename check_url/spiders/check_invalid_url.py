# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
import urllib.parse

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError
from twisted.internet.error import TimeoutError

from check_url.items import CheckUrlItem
from check_url.settings import HEADERS


class CheckInvalidUrlSpider(scrapy.Spider):
    name = 'check_invalid_url'
    allowed_domains = ['example.com']
    # for i in range(1, 8):
    df = pd.read_csv(r'D:\v-baoz\python\check_url\read_csv\decoded_url.csv', sep=',', header=None, encoding='utf-8',
                     names=['urls'])
    start_urls = [url.strip('"') for url in df['urls']]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, headers=HEADERS, callback=self.parse, errback=self.errback_httpbin,
                                 dont_filter=True)

    def parse(self, response):
        item = CheckUrlItem()
        redirect_url_list = response.request.meta.get('redirect_urls')
        if redirect_url_list:
            item['cur_url'] = response.url
            item['redirect_from'] = redirect_url_list[0]
            item['status_code'] = response.status
        else:
            item['cur_url'] = response.url
            item['redirect_from'] = ''
            item['status_code'] = response.status
        yield item

    def errback_httpbin(self, failure):
        # log all errback failures,
        # in case you want to do something special for some errors,
        # you may need the failure's type
        self.logger.error(repr(failure))
        item = CheckUrlItem()
        # if isinstance(failure.value, HttpError):
        if failure.check(HttpError):
            # you can get the response
            response = failure.value.response
            item['cur_url'] = response.url
            item['redirect_from'] = ''
            item['status_code'] = response
            yield item
            # self.logger.error('HttpError on %s', response.url)

        # elif isinstance(failure.value, DNSLookupError):
        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            item['cur_url'] = request.url
            item['redirect_from'] = ''
            item['status_code'] = 'DNSLookupError'
            yield item
            # self.logger.error('DNSLookupError on %s', request.url)

        # elif isinstance(failure.value, TimeoutError):
        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            item['cur_url'] = request.url
            item['redirect_from'] = ''
            item['status_code'] = 'TimeoutError or TCPTimedOutError'
            yield item
            # self.logger.error('TimeoutError on %s', request.url)
        # else:
        #     item['cur_url'] =
        #     item['redirect_from'] = ''
        #     item['status_code'] = 'TimeoutError'
        #     yield item
