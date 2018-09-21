from requests import Session
from requests.exceptions import ReadTimeout
from weixin.config import *
from weixin.db import RedisQueue
from weixin.mysql import MySQL
from weixin.request import WeixinRequest
from urllib import parse
import requests
from pyquery import PyQuery as pq
from requests import ConnectionError


class Spider():
    base_url = 'http://weixin.sogou.com/weixin'
    keyword = 'NBA'
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4,zh-TW;q=0.2,mt;q=0.2',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Cookie': 'ABTEST=8|1537520547|v1; SNUID=D071F262080D7E55EE03CCD708FECF45; IPLOC=CN3100; SUID=D879F5652930990A000000005BA4B3A3; SUID=D879F5652113940A000000005BA4B3A3; JSESSIONID=aaa45tYJMfZ24hiEC3Bvw; SUV=002D1DEC65F579D85BA4B3A4AC47E291',
        'Host': 'weixin.sogou.com',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
        'X-Requested-With':'XMLHttpRequest'
    }
    session = Session()
    queue = RedisQueue()
    mysql = MySQL()
    
    def get_proxy(self):
        """
        从代理池获取代理
        :return:
        """
        try:
            response = requests.get(PROXY_POOL_URL)
            if response.status_code == 200:
                print('Get Proxy', response.text)
                return response.text
            return None
        except requests.ConnectionError:
            return None
    
    def start(self):
        """
        初始化工作
        """
        # 全局更新Headers
        self.session.headers.update(self.headers)
        # 拼接参数
        start_url = self.base_url + '?' + parse.urlencode({'query': self.keyword, 'type': 2})
        # 构建请求对象
        weixin_request = WeixinRequest(url=start_url, callback=self.parse_index, need_proxy=True)
        # 将请求加入到队列中
        self.queue.add(weixin_request)
    
    def parse_index(self, response):
        """
        解析索引页
        :param response: 响应
        :return: 新的响应
        """
        doc = pq(response.text)
        # 解析响应内容,并转为键值对的形式
        items = doc('.news-box .news-list li .txt-box h3 a').items()
        for item in items:
            # 获取详情页URL
            url = item.attr('href')
            # 构建详情页请求
            weixin_request = WeixinRequest(url=url, callback=self.parse_detail)
            yield weixin_request
        # 获取下一页列表页数据
        next = doc('#sogou_next').attr('href')
        if next:
            url = self.base_url + str(next)
            # 构建下一页列表页请求对象
            weixin_request = WeixinRequest(url=url, callback=self.parse_index, need_proxy=True)
            yield weixin_request
    
    def parse_detail(self, response):
        """
        解析详情页
        :param response: 响应
        :return: 微信公众号文章
        """
        # 这里使用PyQuery解析详情页数据
        doc = pq(response.text)
        data = {
            'title': doc('.rich_media_title').text(),
            'content': doc('.rich_media_content').text(),
            'date': doc('#post-date').text(),
            'nickname': doc('#js_profile_qrcode > div > strong').text(),
            'wechat': doc('#js_profile_qrcode > div > p:nth-child(3) > span').text()
        }
        print(data)
        yield data
    
    def request(self, weixin_request):
        """
        执行请求
        :param weixin_request: 请求
        :return: 响应
        """
        try:
            # 如果使用代理
            if weixin_request.need_proxy:
                # 向代理池获取随机代理
                proxy = self.get_proxy()
                if proxy:
                    proxies = {
                        'http': 'http://' + proxy,
                        'https': 'https://' + proxy
                    }
                    return self.session.send(weixin_request.prepare(),timeout=weixin_request.timeout, allow_redirects=False, proxies=proxies)
            return self.session.send(weixin_request.prepare(), timeout=weixin_request.timeout, allow_redirects=False)
        except (ConnectionError, ReadTimeout) as e:
            print(e.args)
            return False
    
    def error(self, weixin_request):
        """
        错误处理
        :param weixin_request: 请求
        :return:
        """
        weixin_request.fail_time = weixin_request.fail_time + 1
        print('Request Failed', weixin_request.fail_time, 'Times', weixin_request.url)
        if weixin_request.fail_time < MAX_FAILED_TIME:
            self.queue.add(weixin_request)
    
    def schedule(self):
        """
        调度请求
        :return:
        """
        while not self.queue.empty():
            # 获取列表中的最后一个请求对象
            weixin_request = self.queue.pop()
            # 获取处理响应的回调函数
            callback = weixin_request.callback
            print('Schedule', weixin_request.url)
            # 发送请求,获取响应
            response = self.request(weixin_request)
            # 如果返回响应成功
            if response and response.status_code in VALID_STATUSES:
                # 解析响应
                results = list(callback(response))
                if results:
                    for result in results:
                        print('New Result', type(result))
                        # 如果结果是请求对象,则添加到请求队列中
                        if isinstance(result, WeixinRequest):
                            self.queue.add(result)
                        # 如果结果是数据,则存入MySql
                        if isinstance(result, dict):
                            self.mysql.insert('articles', result)
                else:
                    self.error(weixin_request)
            else:
                self.error(weixin_request)
    
    def run(self):
        """
        入口
        :return:
        """
        self.start()
        self.schedule()


if __name__ == '__main__':
    spider = Spider()
    spider.run()
