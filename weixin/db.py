from redis import StrictRedis
from weixin.config import *
from pickle import dumps, loads
from weixin.request import WeixinRequest


class RedisQueue():
    def __init__(self):

        # 初始化Redis
        self.db = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)

    def add(self, request):
        """
        向队列添加序列化后的Request
        :param request: 请求对象
        :param fail_time: 失败次数
        :return: 添加结果
        """
        # 如果request的类型与WeixinRequest的类型一致
        if isinstance(request, WeixinRequest):
            # 将请求进行序列化操作,存入Redis列表中,dumps序列化操作是将obj对象转换为二进制对象
            return self.db.rpush(REDIS_KEY, dumps(request))
        return False

    def pop(self):
        """
        取出下一个Request并反序列化
        :return: Request or None
        """
        # 如果Redis列表的长度>0
        if self.db.llen(REDIS_KEY):
            # 移除并返回Redis列表中的最后一个元素,并进行反序列化操作(将二进制对象转换为obj对象)
            return loads(self.db.lpop(REDIS_KEY))
        else:
            return False

    def clear(self):
        self.db.delete(REDIS_KEY)

    def empty(self):
        return self.db.llen(REDIS_KEY) == 0


if __name__ == '__main__':
    db = RedisQueue()
    start_url = 'http://www.baidu.com'
    weixin_request = WeixinRequest(url=start_url, callback='hello', need_proxy=True)
    db.add(weixin_request)
    request = db.pop()
    print(request)
    print(request.callback, request.need_proxy)
