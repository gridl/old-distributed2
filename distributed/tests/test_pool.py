import asyncio
from operator import add
from queue import Queue
from time import time
from toolz import merge

from distributed.center import Center
from distributed.worker import Worker
from distributed.pool import Pool, spawn_loop, divide_tasks, RemoteData
from contextlib import contextmanager

loop = asyncio.get_event_loop()


def test_pool():
    c = Center('127.0.0.1', 8017, loop=loop)

    a = Worker('127.0.0.1', 8018, c.ip, c.port, loop=loop, ncores=1)
    b = Worker('127.0.0.1', 8019, c.ip, c.port, loop=loop, ncores=1)

    p = Pool(c.ip, c.port, loop=loop, start=False)

    @asyncio.coroutine
    def f():
        yield from p._sync_center()

        computation = yield from p._apply_async(add, [1, 2])
        assert computation.status == b'running'
        assert set(p.available_cores.values()) == set([0, 1])
        x = yield from computation._get()
        assert computation.status == x.status == b'success'
        assert list(p.available_cores.values()) == [1, 1]
        result = yield from x._get()
        assert result == 3

        computation = yield from p._apply_async(add, [x, 10])
        y = yield from computation._get()
        result = yield from y._get()
        assert result == 13

        assert set((len(a.data), len(b.data))) == set((0, 2))

        x = yield from p._apply_async(add, [1, 2])
        y = yield from p._apply_async(add, [1, 2])
        assert list(p.available_cores.values()) == [0, 0]
        xx = yield from x._get()
        yield from xx._get()
        assert set(p.available_cores.values()) == set([0, 1])
        yy = yield from y._get()
        yield from yy._get()
        assert list(p.available_cores.values()) == [1, 1]

        seq = yield from p._map(lambda x: x * 100, [1, 2, 3])
        result = yield from seq[0]._get(False)
        assert result == 100
        result = yield from seq[1]._get(False)
        assert result == 200
        result = yield from seq[2]._get(True)
        assert result == 300

        # Handle errors gracefully
        results = yield from p._map(lambda x: 3 / x, [0, 1, 2, 3])
        assert all(isinstance(result, RemoteData) for result in results)
        try:
            yield from results[0]._get()
            assert False
        except ZeroDivisionError:
            pass

        yield from p._close_connections()

        yield from a._close()
        yield from b._close()
        yield from c._close()

    loop.run_until_complete(asyncio.gather(c.go(), a.go(), b.go(), f()))


def test_pool_inputs():
    p = Pool('127.0.0.1:8000', start=False)
    assert p.center_ip == '127.0.0.1'
    assert p.center_port == 8000


def test_pool_thread():
    p = Pool('127.0.0.1', 8000, start=False)
    p.start()
    p.close()


@contextmanager
def cluster(**kwargs):
    loop = asyncio.new_event_loop()
    c = Center('127.0.0.1', 8100, loop=loop)
    a = Worker('127.0.0.1', 8101, c.ip, c.port, loop=loop, **kwargs)
    b = Worker('127.0.0.1', 8102, c.ip, c.port, loop=loop, **kwargs)

    kill_q = Queue()

    @asyncio.coroutine
    def stop():
        while kill_q.empty():
            yield from asyncio.sleep(0.01, loop=loop)
        kill_q.get()

    cor = asyncio.gather(c.go(), a.go(), b.go(), loop=loop)
    cor2 = asyncio.wait([stop(), cor], loop=loop,
            return_when=asyncio.FIRST_COMPLETED)

    thread, loop = spawn_loop(cor, loop)

    try:
        yield c, a, b
    finally:
        if a.status != 'closed':
            a.close()
        if b.status != 'closed':
            b.close()
        c.close()
        kill_q.put(b'')
        # thread.join()


def test_cluster():
    with cluster() as (c, a, b):
        pool = Pool(c.ip, c.port)
        pc = pool.apply_async(add, [1, 2])
        x = pc.get()
        assert x.get() == 3
        assert x.get() == 3

        y = pool.apply(add, [x, x])
        assert y.get() == 6

        pool.close()


def test_error():
    with cluster() as (c, a, b):
        p = Pool(c.ip, c.port)

        results = p.map(lambda x: 3 / x, [0, 1, 2, 3])
        assert all(isinstance(result, RemoteData) for result in results)
        assert results[1].get() == 3 / 1
        try:
            results[0].get()
            assert False
        except ZeroDivisionError:
            pass

        p.close()


def test_workshare():
    who_has = {'x': {'Alice'},
               'y': {'Alice', 'Bob'},
               'z': {'Bob'}}
    needed = {1: {'x'},
              2: {'y'},
              3: {'z'},
              4: {'x', 'z'},
              5: set()}

    shares, extra = divide_tasks(who_has, needed)
    assert shares == {'Alice': [2, 1], 'Bob': [2, 3]}
    assert extra == {4, 5}


def test_map_locality():
    c = Center('127.0.0.1', 8017, loop=loop)

    a = Worker('127.0.0.1', 8018, c.ip, c.port, loop=loop, ncores=4)
    b = Worker('127.0.0.1', 8019, c.ip, c.port, loop=loop, ncores=4)

    p = Pool(c.ip, c.port, loop=loop, start=False)
    @asyncio.coroutine
    def f():
        while len(c.ncores) < 2:
            yield from asyncio.sleep(0.01, loop=loop)

        yield from p._sync_center()

        results = yield from p._map(lambda x: x * 1000, list(range(20)))

        assert p.has_what[(a.ip, a.port)].issuperset(a.data)
        assert p.has_what[(b.ip, b.port)].issuperset(b.data)
        s = {(a.ip, a.port), (b.ip, b.port)}

        assert all(p.who_has[result.key].issubset(s) for result in results)

        results2 = yield from p._map(lambda x: -x, results)

        aval = set(a.data.values())
        bval = set(b.data.values())

        try:
            assert sum(-v in aval for v in aval) > 0.8 * len(aval)
            assert sum(-v in bval for v in bval) > 0.8 * len(bval)
        finally:
            yield from p._close_connections()
            yield from a._close()
            yield from b._close()
            yield from c._close()

    loop.run_until_complete(asyncio.gather(c.go(), a.go(), b.go(), f()))


flag = [0]
maximum = [0]


def test_multiworkers():
    from threading import Lock
    lock = Lock()
    with cluster(ncores=10) as (c, a, b):
        pool = Pool(c.ip, c.port)
        def f(x):
            flag[0] += 1
            from time import sleep
            if maximum[0] < flag[0]:
                maximum[0] = flag[0]
            print(flag[0])
            sleep(0.1)
            flag[0] -= 1
            return x

        start = time()
        result = pool.map(f, list(range(40)))
        end = time()

        assert maximum[0] > 5

        pool.close()


def test_closing_workers():
    with cluster() as (c, a, b):
        p = Pool(c.ip, c.port)

        a.close()

        result = p.map(lambda x: x + 1, range(3))

        assert list(p.available_cores.keys()) == [(b.ip, b.port)]
        p.close()


def test_scatter_delete_sync():
    with cluster() as (c, a, b):
        pool = Pool(c.ip, c.port)

        x, y, z = pool.scatter([1, 2, 3])

        assert set(c.who_has) == {x.key, y.key, z.key}
        assert set.union(*c.who_has.values()).issubset({(a.ip, a.port),
                                                        (b.ip, b.port)})

        y.delete()

        assert set(c.who_has) == {x.key, z.key}
        assert set.union(*c.who_has.values()).issubset({(a.ip, a.port),
                                                        (b.ip, b.port)})
        assert merge(a.data, b.data) == {x.key: 1, z.key: 3}

        X, Z = pool.collect([x, z])
        assert [X, Z] == [1, 3]

        pool.close()
