import pandas as pd

import urllib.parse


def read_url_from_csv():
    chunksize = 40000
    reader = pd.read_csv('need_check_urls.tsv', sep='\t', header=None, encoding='utf-8', chunksize=chunksize,
                         usecols=[2],
                         names=['urls'])
    i = 0
    for chunk in reader:
        url_decode = [urllib.parse.unquote(str(url)).strip('"') for url in chunk['urls']]
        chunk['url_decode'] = url_decode
        # chunk.shape[0] row number
        i = i + 1
        chunk.to_csv('url_{}.tsv'.format(i), index=False, header=False, columns=['url_decode'])


if __name__ == '__main__':
    read_url_from_csv()
