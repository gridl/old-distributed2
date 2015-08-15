from functools import partial
import asyncio
from collections import defaultdict

from dist.core import serve, read, write, connect
from dist.center import manage_metadata

loop = asyncio.get_event_loop()


def test_metadata():
    who_has = defaultdict(set)
    has_what = defaultdict(set)
    handlers = {'register': partial(manage_metadata, who_has, has_what),
                'unregister': partial(manage_metadata, who_has, has_what),
                'who-has': partial(manage_metadata, who_has, has_what),
                'has-what': partial(manage_metadata, who_has, has_what)}

    s = serve('*', 8006, handlers, loop=loop)

    @asyncio.coroutine
    def f():
        reader, writer = yield from connect('127.0.0.1', 8006, loop=loop)

        msg = {'op': 'register',
               'address': 'alice',
               'keys': ['x', 'y'],
               'reply': True}
        yield from write(writer, msg)
        response = yield from read(reader)
        assert response == b'OK'

        msg = {'op': 'register',
               'address': 'bob',
               'keys': ['y', 'z'],
               'reply': True}
        yield from write(writer, msg)
        response = yield from read(reader)
        assert response == b'OK'

        msg = {'op': 'who-has',
               'keys': ['x', 'y']}
        yield from write(writer, msg)
        response = yield from read(reader)
        assert response == {'x': set(['alice']), 'y': set(['alice', 'bob'])}

        msg = {'op': 'unregister',
               'address': 'bob',
               'keys': ['y'],
               'reply': True}
        yield from write(writer, msg)
        response = yield from read(reader)
        assert response == b'OK'

        msg = {'op': 'has-what',
               'keys': ['alice', 'bob'],
               'close': True}
        yield from write(writer, msg)
        response = yield from read(reader)
        assert response == {'alice': set(['x', 'y']), 'bob': set(['z'])}

    loop.run_until_complete(asyncio.gather(s, f()))
