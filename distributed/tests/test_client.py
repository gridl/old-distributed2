import asyncio

from toolz import merge

from distributed import Center, Worker
from distributed.client import scatter_to_center, scatter_to_workers
from distributed.core import sync


loop = asyncio.get_event_loop()

def test_scatter_delete():
    c = Center('127.0.0.1', 8017, loop=loop)
    a = Worker('127.0.0.1', 8018, c.ip, c.port, loop=loop, ncores=1)
    b = Worker('127.0.0.1', 8019, c.ip, c.port, loop=loop, ncores=1)

    @asyncio.coroutine
    def f():
        while len(c.ncores) < 2:
            yield from asyncio.sleep(0.01, loop=loop)
        data = yield from scatter_to_center(c.ip, c.port, [1, 2, 3], loop=loop)

        assert merge(a.data, b.data) == \
                {d.key: i for d, i in zip(data, [1, 2, 3])}

        assert set(c.who_has) == {d.key for d in data}
        assert all(len(v) == 1 for v in c.who_has.values())

        assert [d.get() for d in data] == [1, 2, 3]

        yield from data[0]._delete()

        assert merge(a.data, b.data) == \
                {d.key: i for d, i in zip(data[1:], [2, 3])}

        assert data[0].key not in c.who_has

        data = yield from scatter_to_workers(c.ip, c.port,
                [a.address, b.address], [4, 5, 6], loop=loop)

        m = merge(a.data, b.data)

        for d, v in zip(data, [4, 5, 6]):
            assert m[d.key] == v

        yield from a._close()
        yield from b._close()
        yield from c._close()

    loop.run_until_complete(asyncio.gather(c.go(), a.go(), b.go(), f()))


def test_sync_interactively():
    c = Center('127.0.0.1', 8017, start=True, block=False)
    a = Worker('127.0.0.1', 8018, c.ip, c.port, ncores=1,
            start=True, block=False)
    b = Worker('127.0.0.1', 8019, c.ip, c.port, ncores=1,
            start=True, block=False)

    try:
        values = [1, 2, 3, 4, 5, 6, 7, 8]
        data = sync(scatter_to_center(c.ip, c.port, values))
        assert merge(a.data, b.data) == {d.key: v for d, v in zip(data,
            values)}
    finally:
        a.close()
        b.close()
        c.close()
