import asyncio
import random

from toolz import merge

from .core import rpc

@asyncio.coroutine
def collect_from_center(loop, ip, port, needed=None):
    """ Collect data from peers """
    who_has = yield from rpc(ip, port).who_has(keys=needed, loop=loop)

    assert set(who_has) == set(needed)

    result = yield from collect_from_workers(loop, who_has)
    return result


@asyncio.coroutine
def collect_from_workers(loop, who_has):
    """ Collect data from peers """
    coroutines = [rpc(*random.choice(list(addresses))).get_data(
                        keys=[key], loop=loop)
                        for key, addresses in who_has.items()]

    results = yield from asyncio.gather(*coroutines, loop=loop)

    # TODO: make resilient to missing workers
    return merge(results)
