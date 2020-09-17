# -*- coding: utf-8 -*-

import threading
import time
import queue


class TooManyConnections(Exception):
    '''当连接数过多时，抛出该异常'''


class Expired(Exception):
    '''当连接不可用时，抛出该异常'''


class UsageExceeded(Expired):
    '''连接的使用次数超出限制'''


class TtlExceeded(Expired):
    '''连接使用时间超出 ttl 指定的生命周期'''


class IdleExceeded(Expired):
    '''闲置时间超出 idle 指定的时间'''


class WrapperConnection(object):
    '''用于包装连接池中的数据库连接，以便处理生命周期逻辑'''

    def __init__(self, pool, connection):
        self.pool = pool
        self.connection = connection
        self.usage = 0
        self.last = self.created = time.time()

    def using(self):
        '''当连接被调用时使用该方法，使用次数 +1'''
        self.usage += 1
        self.last = time.time()
        return self

    def reset(self):
        '''重置连接包装状态'''
        self.usage = self.last = self.created = 0

    def __enter__(self):
        return self.connection

    def __exit__(self, *exc_info):
        self.pool.release(self)


class ConnectionPool(object):
    '''连接池类，可用于 pymysql/memcache/redis/... 等

    可通过如下方式调用：
        pool = ConnectionPool(create=redis.Redis)

    也可以通过 lambda 的方式指定 create 调用：
        pool = ConnectionPool(create=lambda: redis.Redis(host='127.0.0.1'))

    或者通过 functools.partial
        from functools import partial
        pool = ConnectionPool(create=partial(redis.Redis, host='127.0.0.1'))
    '''

    __wrappers = {}

    def __init__(self, create, close=None, max_size=10, max_usage=0, ttl=0, idle=60, block=True):
        '''初始化参数

            create:    必须是一个可 callback 的函数
                       must be a callback function, creates connection
            close:     optional callback to close connection
            max_size:  最大连接数，当为 0 的时候则没有限制，不建议设置为 0
                       the maximum number of connections, 0 is no limit (not recommended)
            max_usage: 连接可使用次数，达到该次数后，连接将被释放/关闭
                       the number of times the connection can be used,
                       after this number of times, the connection will be released/closed
            ttl:       连接可使用时间，单位(秒)，当连接使用达到指定时间后，
                       连接将被释放/关闭
                       connection use time limit (seconds), when reached, the connection
                       will be released/closed
            idle:      连接空闲时间，单位(秒)，当连接在闲置指定时间后，将被释放/关闭
                       connection idle time (seconds), when reached, the connection
                       will be released/closed
            block:     当连接数满的时候，是否阻塞等待连接被释放,
                       输入 False 则在连接池满时会抛出异常
                       When the number of connections is full,
                       whether to block and wait for the connection to be released.
                       Enter False to throw an exception when the connection pool is full
        '''
        if not hasattr(create, '__call__'):
            raise ValueError('"create" argument is not callable')

        if close is not None and not hasattr(close, '__call__'):
            raise ValueError('"close" argument is not callable')

        self._create = create
        self._close = close
        self._max_size = int(max_size)
        self._max_usage = int(max_usage)
        self._ttl = int(ttl)
        self._idle = int(idle)
        self._block = bool(block)
        self._lock = threading.Condition()
        self._pool = queue.Queue()
        self._size = 0

    def item(self):
        '''可通过 with ... as ... 语法调用

            pool = ConnectionPool(create=redis.Redis)
            with pool.item() as redis:
                redis.set('foo', 'bar)
        '''
        self._lock.acquire()

        try:
            while (self._max_size and self._pool.empty() and self._size >= self._max_size):
                if not self._block:
                    raise TooManyConnections('Too many connections')

                self._lock.wait()  # 等待闲置连接

            try:
                wrapped = self._pool.get_nowait()  # 从空闲连接池中获取一个
                if self._idle and (wrapped.last + self._idle) < time.time():
                    self._destroy(wrapped)
                    raise IdleExceeded('Idle exceeds %d secs' % self._idle)
            except (queue.Empty, IdleExceeded):
                wrapped = self._wrapper(self._create())  # 创建新连接
                self._size += 1
        finally:
            self._lock.release()

        return wrapped.using()

    def release(self, conn):
        '''释放一个连接，让连接重回到连接池中

        当连接使用超过限制/超过限定时间时，连接将被销毁
        '''
        self._lock.acquire()
        wrapped = self._wrapper(conn)

        try:
            self._test(wrapped)
        except Expired:
            self._destroy(wrapped)
        else:
            self._pool.put_nowait(wrapped)
            self._lock.notifyAll()  # 通知其它线程，有闲置连接可用
        finally:
            self._lock.release()

    def _destroy(self, wrapped):
        '''销毁一个连接'''
        if self._close:
            self._close(wrapped.connection)

        self._unwrapper(wrapped)
        self._size -= 1

    def _wrapper(self, conn):
        '''利用 id 地址，对连接进行包装'''
        if isinstance(conn, WrapperConnection):
            return conn

        _id = id(conn)

        if _id not in self.__wrappers:
            self.__wrappers[_id] = WrapperConnection(self, conn)

        return self.__wrappers[_id]

    def _unwrapper(self, wrapped):
        '''取消对连接的包装'''
        if not isinstance(wrapped, WrapperConnection):
            return

        _id = id(wrapped.connection)
        wrapped.reset()
        del wrapped

        if _id in self.__wrappers:
            del self.__wrappers[_id]

    def _test(self, wrapped):
        '''测试连接的可用性，当不可用时，抛出 Expired 异常'''
        if self._max_usage and wrapped.usage >= self._max_usage:
            raise UsageExceeded('Usage exceeds %d times' % self._max_usage)

        if self._ttl and (wrapped.created + self._ttl) < time.time():
            raise TtlExceeded('TTL exceeds %d secs' % self._ttl)
