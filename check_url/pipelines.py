# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pandas as pd
import os


class CheckUrlPipeline(object):
    flag = 0

    def process_item(self, item, spider):
        df = pd.DataFrame({
            'cur_url': [item['cur_url']],
            'redirect_from': [item['redirect_from']],
            'status_code': [item['status_code']],
        })

        if self.flag < 40000:
            if not os.path.exists('url_status1.csv'):
                df.to_csv('url_status1.csv', index=False, header=True, encoding='utf-8', mode='a')
            else:
                df.to_csv('url_status1.csv', index=False, header=None, encoding='utf-8', mode='a')
        elif 40000 < self.flag < 80000:
            if not os.path.exists('url_status2.csv'):
                df.to_csv('url_status2.csv', index=False, header=True, encoding='utf-8', mode='a')
            else:
                df.to_csv('url_status2.csv', index=False, header=None, encoding='utf-8', mode='a')
        elif 80000 < self.flag < 120000:
            if not os.path.exists('url_status3.csv'):
                df.to_csv('url_status3.csv', index=False, header=True, encoding='utf-8', mode='a')
            else:
                df.to_csv('url_status3.csv', index=False, header=None, encoding='utf-8', mode='a')
        elif 120000 < self.flag < 160000:
            if not os.path.exists('url_status4.csv'):
                df.to_csv('url_status4.csv', index=False, header=True, encoding='utf-8', mode='a')
            else:
                df.to_csv('url_status4.csv', index=False, header=None, encoding='utf-8', mode='a')
        elif 160000 < self.flag < 200000:
            if not os.path.exists('url_status5.csv'):
                df.to_csv('url_status5.csv', index=False, header=True, encoding='utf-8', mode='a')
            else:
                df.to_csv('url_status5.csv', index=False, header=None, encoding='utf-8', mode='a')
        elif 200000 < self.flag < 270000:
            if not os.path.exists('url_status6.csv'):
                df.to_csv('url_status6.csv', index=False, header=True, encoding='utf-8', mode='a')
            else:
                df.to_csv('url_status6.csv', index=False, header=None, encoding='utf-8', mode='a')

    flag += 1
