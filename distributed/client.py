import asyncio
import random
import uuid
from itertools import count, cycle
from collections import Iterable, defaultdict

from toolz import merge, concat, groupby

from .core import rpc, sync


no_default = '__no_default__'

@asyncio.coroutine
def collect_from_center(ip, port, needed=None, loop=None):
    """ Collect data from peers """
    needed = [n.key if isinstance(n, RemoteData) else n for n in needed]
    who_has = yield from rpc(ip, port).who_has(keys=needed, loop=loop)

    assert set(who_has) == set(needed)

    result = yield from collect_from_workers(who_has, loop=loop)
    return [result[key] for key in needed]


@asyncio.coroutine
def collect_from_workers(who_has, loop=None):
    """ Collect data from peers """
    d = defaultdict(list)
    for key, addresses in who_has.items():
        addr = random.choice(list(addresses))
        d[addr].append(key)

    print(who_has)
    print(d)

    coroutines = [rpc(*addr).get_data(keys=keys, loop=loop)
                        for addr, keys in d.items()]

    results = yield from asyncio.gather(*coroutines, loop=loop)

    # TODO: make resilient to missing workers
    return merge(results)


class RemoteData(object):
    """ Data living on a remote worker

    This is created by ``PendingComputation.get()`` which is in turn created by
    ``Pool.apply_async()``.  One can retrive the data from the remote worker by
    calling the ``.get()`` method on this object

    Example
    -------

    >>> pc = pool.apply_async(func, args, kwargs)  # doctest: +SKIP
    >>> rd = pc.get()  # doctest: +SKIP
    >>> rd.get()  # doctest: +SKIP
    10
    """
    def __init__(self, key, center_ip, center_port, loop=None, status=None,
            result=no_default):
        self.key = key
        self.loop = loop
        self.status = status
        self.center_ip = center_ip
        self.center_port = center_port
        self._result = result

    @asyncio.coroutine
    def _get(self, raiseit=True):
        who_has = yield from rpc(self.center_ip, self.center_port).who_has(
                keys=[self.key], close=True)
        ip, port = random.choice(list(who_has[self.key]))
        result = yield from rpc(ip, port).get_data(keys=[self.key], close=True)

        self._result = result[self.key]

        if raiseit and self.status == b'error':
            raise self._result
        else:
            return self._result

    def get(self):
        if self._result is not no_default:
            return self._result
        else:
            result = sync(self._get(raiseit=False), self.loop)
            if self.status == b'error':
                raise result
            else:
                return result

    @asyncio.coroutine
    def _delete(self):
        yield from rpc(self.center_ip, self.center_port).delete_data(
                keys=[self.key])

    def delete(self):
        sync(self._delete(), self.loop)


@asyncio.coroutine
def scatter_to_center(ip, port, data, key=None, loop=None):
    """ Scatter data to workers

    See also:
        scatter_to_workers
    """
    ncores = yield from rpc(ip, port).ncores()

    result = yield from scatter_to_workers(ip, port, ncores, data, key=key, loop=loop)
    return result


@asyncio.coroutine
def scatter_to_workers(ip, port, ncores, data, key=None, loop=None):
    """ Scatter data directly to workers

    This distributes data in a round-robin fashion to a set of workers based on
    how many cores they have.

    See also:
        scatter_to_center: check in with center first to find workers
    """
    if key is None:
        key = str(uuid.uuid1())

    if isinstance(ncores, Iterable) and not isinstance(ncores, dict):
        ncores = {worker: 1 for worker in ncores}

    workers = list(concat([w] * nc for w, nc in ncores.items()))
    names = ('%s-%d' % (key, i) for i in count(0))

    L = list(zip(cycle(workers), names, data))
    d = groupby(0, L)
    d = {k: {b: c for a, b, c in v}
          for k, v in d.items()}

    coroutines = [rpc(*w).update_data(data=v)
                  for w, v in d.items()]

    yield from asyncio.gather(*coroutines, loop=loop)

    result = [RemoteData(b, ip, port, loop, result=c)
                for a, b, c in L]

    return result
