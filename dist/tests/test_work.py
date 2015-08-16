from functools import partial
import asyncio
from collections import defaultdict
from operator import add

from dist.core import serve, read, write, connect, manage_data
from dist.center import manage_metadata
from dist.work import work

loop = asyncio.get_event_loop()


def test_worker():
    who_has = defaultdict(set)
    has_what = defaultdict(set)
    m_port = 8007
    handlers = {'register': partial(manage_metadata, who_has, has_what),
                'unregister': partial(manage_metadata, who_has, has_what),
                'who-has': partial(manage_metadata, who_has, has_what),
                'has-what': partial(manage_metadata, who_has, has_what)}

    metadata = serve('*', m_port, handlers, loop=loop)

    a_data = dict()
    a_port = 8008
    handlers = {'compute': partial(work, loop, a_data, '127.0.0.1', a_port,
                                                       '127.0.0.1', m_port),
                'get-data': partial(manage_data, a_data),
                'update-data': partial(manage_data, a_data),
                'del-data': partial(manage_data, a_data)}
    a = serve('*', a_port, handlers, loop=loop)

    b_data = dict()
    b_port = 8009
    handlers = {'compute': partial(work, loop, b_data, '127.0.0.1', b_port,
                                                       '127.0.0.1', m_port),
                'get-data': partial(manage_data, b_data),
                'update-data': partial(manage_data, b_data),
                'del-data': partial(manage_data, b_data)}
    b = serve('*', b_port, handlers, loop=loop)

    @asyncio.coroutine
    def f():
        a_reader, a_writer = yield from connect('127.0.0.1', a_port, loop=loop)
        b_reader, b_writer = yield from connect('127.0.0.1', b_port, loop=loop)

        msg = {'op': 'compute',
               'key': 'x',
               'function': add,
               'args': [1, 2],
               'needed': [],
               'reply': True}
        yield from write(a_writer, msg)
        response = yield from read(a_reader)
        assert response == b'OK'
        assert a_data['x'] == 3
        assert who_has['x'] == set([('127.0.0.1', a_port)])

        msg = {'op': 'compute',
               'key': 'y',
               'function': add,
               'args': ['x', 10],
               'needed': ['x'],
               'reply': True}
        yield from write(b_writer, msg)
        response = yield from read(b_reader)
        assert response == b'OK'
        assert b_data['y'] == 13
        assert who_has['y'] == set([('127.0.0.1', b_port)])

        yield from write(a_writer, {'op': 'close', 'reply': True})
        yield from write(b_writer, {'op': 'close', 'reply': True})
        yield from read(a_reader)
        yield from read(b_reader)

        a_writer.close()
        b_writer.close()

    loop.run_until_complete(asyncio.gather(metadata, a, b, f()))
