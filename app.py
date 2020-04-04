import requests, threading, pandas
from datetime import datetime
from time import time
from concurrent.futures import ThreadPoolExecutor

requests.adapters.DEFAULT_RETRIES = 5


class Spider:
    def __init__(self, m, t):
        self.media_id = m
        self.cursor = 0
        self.s = requests.session()
        self.s.keep_alive = False
        self.cnt = 0
        self.pool = ThreadPoolExecutor(8)
        self.lock = threading.Lock()
        self.typ = t
        with open(t + '.csv', 'a', encoding='utf_8_sig') as f:
            f.write('点评内容,点评时间,点评星数,点评获赞量\n')

    def crawler(self):
        while True:
            url = "https://api.bilibili.com/pgc/review/{}/list?" \
                  "media_id={}" \
                  "&ps=20&sort=0&" \
                  "cursor={}".format(self.typ, self.media_id, self.cursor)
            res = self.s.get(url).json()['data']
            if res['next'] == 0:
                break
            self.cursor = res['next']
            self.pool.submit(self.store, res['list'])

    def store(self, gen):
        for li in gen:
            date_array = datetime.utcfromtimestamp(li['mtime'])
            date = date_array.strftime('%Y-%m-%d %H:%M:%S')
            # write in excel
            df = pandas.DataFrame([{
                '点评时间': date,
                '点评星数': li['score'] / 2,
                '点评内容': li['content'],
                '点评获赞量': li['stat']['likes']
            }])
            self.lock.acquire()
            try:
                self.cnt += 1
                df.to_csv(self.typ + '.csv', mode='a', index=False, header=False,
                          columns=['点评内容', '点评时间', '点评星数', '点评获赞量'],
                          encoding='utf_8_sig')
            except Exception as e:
                print(e)
            finally:
                self.lock.release()
            if self.cnt % 3000 == 0:
                print('{} now: {}'.format(self.typ, self.cnt))


print('id号样例：https://www.bilibili.com/bangumi/media/md?????????  问号处的数字为id号')
media_id = input('请输入作品的id号：')
short_spider = Spider(media_id, 'short')
long_spider = Spider(media_id, 'long')

start = time()
with ThreadPoolExecutor(2) as pool:
    f1 = pool.submit(short_spider.crawler)
    f2 = pool.submit(long_spider.crawler)

end = time()
print('---------------FIN---------------------')
print('耗时 {}'.format(end - start))
