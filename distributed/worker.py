import asyncio
import random
from toolz import merge, partial

from .core import (read, write, connect, delay, manage_data, serve, send_recv,
        spawn_loop)


class Worker(object):
    """ Worker node in a distributed network

    Workers do the following:

    1.  Manage and serve from a dictionary of local data
    2.  Perform computations on that data and on data from peers
    3.  Interact with peers and with a ``Center`` node to acheive 2

    A worker should connect to a ``Center`` node.  It can run in an event loop
    or separately in a thread.

    Example
    -------

    Set up a Center on a separate machine

    >>> c = Center('192.168.0.100', 8000)  # doctest: +SKIP

    Run in an event loop

    >>> w = Worker('192.168.0.101', 8001,
    ...            center_ip='192.168.0.100', center_port=8000) # doctest: +SKIP
    >>> coroutine = w.go()

    Can run separately in a thread

    >>> w = Worker('192.168.0.101', 8001,
    ...            center_ip='192.168.0.100', center_port=8000,
    ...            start=True, block=False)  # doctest: +SKIP
    """

    def __init__(self, ip, port, center_ip, center_port, bind='*', loop=None,
                 start=False, block=True):
        self.ip = ip
        self.port = port
        self.center_ip = center_ip
        self.center_port = center_port
        self.bind = bind
        self.loop = loop or asyncio.new_event_loop()
        self.data = dict()

        if start:
            self.start(block)

    @asyncio.coroutine
    def go(self):
        data_cor = partial(manage_data, self.data)
        work_cor = partial(work, self.loop, self.data, self.ip, self.port,
                                 self.center_ip, self.center_port)
        handlers = {'compute': work_cor,
                    'get-data': data_cor,
                    'update-data': data_cor,
                    'del-data': data_cor}

        resp = yield from send_recv(self.center_ip, self.center_port,
                                    op='register', address=(self.ip, self.port),
                                    reply=True, close=True)
        assert resp == b'OK'

        self.server = yield from serve(self.bind, self.port, handlers, loop=self.loop)
        yield from self.server.wait_closed()

    def start(self, block):
        """ Start worker.

        If block is false then run the event loop in a separate thread
        """
        if block:
            self.loop.run_until_complete(self.go())
        else:
            self._thread, _ = spawn_loop(self.go(), loop=self.loop)

    def close(self):
        self.loop.call_soon_threadsafe(self.server.close)
        if hasattr(self, '_thread'):
            self._thread.join()


@asyncio.coroutine
def collect(loop, reader, writer, needed):
    who_has = yield from send_recv(reader, writer, op='who-has', keys=needed,
            reply=True)
    assert set(who_has) == set(needed)

    # TODO: This should all be done in parallel and in fewer messages
    results = []
    for key, addresses in who_has.items():
        host, port = random.choice(list(addresses))
        result = yield from send_recv(host, port, op='get-data', keys=[key],
                loop=loop, reply=True, close=True)
        results.append(result)

    # TODO: Update metadata to say that we have this data

    return merge(results)


@asyncio.coroutine
def work(loop, data, ip, port, metadata_ip, metadata_port, reader, writer, msg):
    m_reader, m_writer = yield from connect(metadata_ip, metadata_port, loop)
    assert msg['op'] == 'compute'

    # Unpack message
    function = msg['function']
    key = msg['key']
    args = msg.get('args', ())
    kwargs = msg.get('kwargs', {})
    needed = msg.get('needed', [])

    # Collect data from peers
    if msg['needed']:
        other = yield from collect(loop, m_reader, m_writer, needed)
        data2 = merge(data, other)
    else:
        data2 = data

    # Fill args with data, compute in separate thread
    args2 = keys_to_data(args, data2)
    kwargs2 = keys_to_data(kwargs, data2)
    result = yield from delay(loop, function, *args2, **kwargs)
    data[key] = result

    # Tell center about or new data
    response = yield from send_recv(m_reader, m_writer, op='add-keys',
            address=(ip, port), keys=[key], reply=True, close=True)
    assert response == b'OK'  # TODO: do this asynchronously?

    if msg.get('reply'):
        yield from write(writer, b'OK')


def keys_to_data(o, data):
    """

    >>> keys_to_data(('x', 'y'), {'x': 1})
    (1, 'y')
    >>> keys_to_data({'a': 'x', 'b': 'y'}, {'x': 1})
    {'a': 1, 'b': 'y'}
    """
    if isinstance(o, (tuple, list)):
        result = []
        for arg in o:
            try:
                result.append(data[arg])
            except (TypeError, KeyError):
                result.append(arg)
        result = type(o)(result)

    if isinstance(o, dict):
        result = {}
        for k, v in o.items():
            try:
                result[k] = data[v]
            except (TypeError, KeyError):
                result[k] = v
    return result