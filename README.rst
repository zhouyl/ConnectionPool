ConnectionPool
##############

Thread-safe connection pool for python

Install
=======

.. code-block:: bash

    $ pip install connection_pool


Examples
========

Create a pool
---------------

.. code-block:: python

    import memcache
    from connection_pool import ConnectionPool

    # via create function
    def create_memcache_client():
        return memcache.Client(['127.0.0.1:11211'])

    pool = ConnectionPool(create=create_memcache_client,
                          max_size=10, max_usage=10000, idle=60, ttl=120)

    # via lambda
    pool = ConnectionPool(create=lambda: memcache.Client(['127.0.0.1:11211']),
                          max_size=10)

    # via functools.partial
    from functools import partial
    pool = ConnectionPool(create=partial(memcache.Client, ['127.0.0.1:11211']),
                          max_size=10)

    # using a connection
    with pool.item() as memcache:
        memcache.set('foo', 'bar')

License
=======

The MIT License (MIT). Please see License File for more information.
