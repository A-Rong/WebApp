# -*- coding: utf-8 -*-
# @Time    : 2018/3/9 20:39

import logging; logging.basicConfig(level=logging.INFO)

import asyncio, os , json, time
from datetime import datetime

from aiohttp import web

def index(request):
    return web.Response(body='<h1>Awesome</h1>')


# 用asyncio提供的@asyncio.coroutine可以把一个generator标记为coroutine类型，然后在coroutine内部用yield from调用另一个coroutine实现异步操作。
@asyncio.coroutine
def init(loop):
    app = web.Application(loop = loop)
    app.router.add_route('GET', '/', index)     #对于首页的 / 请求进行相应，响应方法为上面的index的函数
    srv = yield from loop.create_server(app.make_handler(), '127.0.0.1', 9000)  #创建服务器
    logging.info('server started at http://127.0.0.1:9000....')
    return srv

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()