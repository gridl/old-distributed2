import asyncio
from collections import namedtuple
import random
from queue import Queue
import uuid

from toolz import merge, partial, pipe, concat, frequencies, concat
from toolz.curried import map, filter

from .core import read, write, connect, send_recv, spawn_loop, sync


class Pool(object):
    """ Remote computation pool

    This connects to a metadata ``Center`` and from there learns to where it can
    dispatch jobs, typically through an ``apply_async`` call.

    Example
    -------

    >>> pool = Pool(center_ip='192.168.0.100', center_port=8000)  # doctest: +SKIP

    >>> pc = pool.apply_async(func, args, kwargs)  # doctest: +SKIP
    >>> rd = pc.get()  # doctest: +SKIP
    >>> rd.get()  # doctest: +SKIP
    10
    """
    def __init__(self, center_ip, center_port, loop=None, start=True):
        self.center_ip = center_ip
        self.center_port = center_port
        self.loop = loop or asyncio.new_event_loop()
        self._reader_writers = set()

        if start:
            self.start()
            self.sync_center()

    @asyncio.coroutine
    def _sync_center(self):
        reader, writer = yield from connect(self.center_ip, self.center_port,
                                            loop=self.loop)
        self.who_has = yield from send_recv(reader, writer, op='who-has',
                                            reply=True)
        self.has_what = yield from send_recv(reader, writer, op='has-what',
                                             reply=True)
        self.available_cores = yield from send_recv(reader, writer,
                            op='ncores', reply=True, close=True)
        writer.close()

    @asyncio.coroutine
    def _map(self, func, seq, **kwargs):
        tasks = []
        for i, item in enumerate(seq):
            needed, args2, kwargs2 = needed_args_kwargs((item,), kwargs)
            tasks.append(dict(key=str(uuid.uuid1()),
                              function=func, args=args2,
                              kwargs=kwargs2,
                              needed=needed, index=i))

        output = [None for i in seq]
        seen = set()
        needed = {i: task['needed'] for i, task in enumerate(tasks)}

        shares, extra = divide_tasks(self.who_has, needed)

        coroutines = list(concat([[
            handle_worker(self.loop, tasks, shares, extra, seen, output, worker)
            for i in range(count)]
            for worker, count in self.available_cores.items()]))

        reader_writers = yield from asyncio.gather(*coroutines)
        assert all(isinstance(o, RemoteData) for o in output)

        self._reader_writers.update(reader_writers)

        return output


    def sync_center(self):
        """ Get who_has and has_what dictionaries from a center

        In particular this tells us what workers we have at our disposal
        """
        cor = self._sync_center()
        return sync(self.loop, cor)

    def start(self):
        """ Start an event loop in a thread """
        self._kill_q = Queue()

        @asyncio.coroutine
        def f():
            while self._kill_q.empty():
                yield from asyncio.sleep(0.01)
            self._kill_q.get()

        self._thread, _ = spawn_loop(f(), loop=self.loop)

    @asyncio.coroutine
    def _close_connections(self):
        """ Close active connections """
        for reader, writer in self._reader_writers:
            if writer.transport._sock:
                result = yield from send_recv(reader, writer, op='close',
                                              reply=True, close=True)

    def close_connections(self):
        sync(self.loop, self._close_connections())


    def close(self):
        """ Close the thread that manages our event loop """
        self.close_connections()
        if hasattr(self, '_thread'):
            self._kill_q.put('')
            self._thread.join()

    @asyncio.coroutine
    def _apply_async(self, func, args=(), kwargs={}, key=None):
        needed, args2, kwargs2 = needed_args_kwargs(args, kwargs)

        ip, port = choose_worker(needed, self.who_has, self.has_what,
                                 self.available_cores)

        if key is None:
            key = str(uuid.uuid1())

        pc = PendingComputation(key, func, args2, kwargs2, needed,
                                loop=self.loop)
        yield from pc._start(ip, port, self.who_has, self.has_what,
                             self.available_cores)
        return pc

    def apply_async(self, func, args=(), kwargs={}, key=None):
        """ Execute a function in a remote worker

        If an arg or a kwarg is a ``RemoteData`` object then that data will be
        communicated as necessary among the ``Worker`` peers.
        """
        cor = self._apply_async(func, args, kwargs, key)
        return sync(self.loop, cor)


class PendingComputation(object):
    """ A future for a computation that done in a remote worker

    This is generally created by ``Pool.apply_async``.  It can be converted
    into a ``RemoteData`` object by calling the ``.get()`` method.

    Example
    -------

    >>> pc = pool.apply_async(func, args, kwargs)  # doctest: +SKIP
    >>> rd = pc.get()  # doctest: +SKIP
    >>> rd.get()  # doctest: +SKIP
    10
    """
    def __init__(self, key, function, args, kwargs, needed, loop=None):
        self.key = key
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.needed = needed
        self.loop = loop

    @asyncio.coroutine
    def _start(self, ip, port, who_has=None, has_what=None,
               available_cores=None):
        msg = dict(op='compute', key=self.key, function=self.function,
                   args=self.args, kwargs=self.kwargs, needed=self.needed,
                   reply=True)
        self.ip = ip
        self.port = port
        self._has_what = has_what
        self._who_has = who_has
        self._available_cores = available_cores
        self.reader, self.writer = yield from connect(ip, port)
        self._available_cores[(ip, port)] -= 1
        yield from write(self.writer, msg)

    @asyncio.coroutine
    def _get(self):
        result = yield from read(self.reader)
        assert result == b'OK'
        self._who_has[self.key].add((self.ip, self.port))
        self._has_what[(self.ip, self.port)].add(self.key)
        self._available_cores[(self.ip, self.port)] += 1
        self._result = RemoteData(self.key, reader=self.reader,
                                  writer=self.writer, loop=self.loop)
        return self._result

    def get(self):
        try:
            return self._result
        except AttributeError:
            return sync(self.loop, self._get())


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
    def __init__(self, key, reader=None, writer=None, loop=None):
        self.key = key
        self.reader = reader
        self.writer = writer
        self.loop = loop

    @asyncio.coroutine
    def _get(self, close=True):
        result = yield from send_recv(self.reader, self.writer, op='get-data',
                                      keys=[self.key], reply=True, close=close)
        if close:
            self.writer.close()
        self._result = result[self.key]
        return self._result

    def get(self, close=True):
        try:
            return self._result
        except AttributeError:
            return sync(self.loop, self._get(close))


def choose_worker(needed, who_has, has_what, available_cores):
    """ Select worker to run computation

    Currently selects the worker with the most data local
    """
    workers = {w for w, c in available_cores.items() if c}
    counts = pipe(needed, map(who_has.__getitem__), concat,
            filter(workers.__contains__), frequencies)
    if not counts:
        return random.choice(list(workers))
    else:
        biggest = max(counts.values())
        best = {k: v for k, v in counts.items() if v == biggest}
        return random.choice(list(best))


def needed_args_kwargs(args, kwargs):
    """ Replace RemoteData objects with keys, fill needed """
    needed = set()
    args2 = []
    for arg in args:
        if isinstance(arg, RemoteData):
            args2.append(arg.key)
            needed.add(arg.key)
        else:
            args2.append(arg)

    kwargs2 = {}
    for k, v in kwargs.items():
        if isinstance(arg, RemoteData):
            kwargs2[k] = arg.key
            needed.add(arg.key)
        else:
            kwargs2[k] = arg

    return needed, args2, kwargs2


def divide_tasks(who_has, needed):
    """ Divvy up work between workers

    Given the following dictionaries:

    who_has: {data: [workers who have data]}
    needed: {task: [data required by task]}

    Produce a dictionary of tasks for each worker to do in sorted order of
    priority.  These lists of tasks may overlap.

    Example
    -------

    >>> who_has = {'x': {'Alice'},
    ...            'y': {'Alice', 'Bob'},
    ...            'z': {'Bob'}}
    >>> needed = {1: {'x'},       # doable by Alice
    ...           2: {'y'},       # doable by Alice and Bob
    ...           3: {'z'},       # doable by Bob
    ...           4: {'x', 'z'},  # doable by neither
    ...           5: set()}       # doable by all
    >>> shares, extra = divide_tasks(who_has, needed)
    >>> shares  # doctest: +SKIP
    {'Alice': [2, 1],
       'Bob': [2, 3]}
    >>> extra
    {4, 5}

    Ordering
    --------

    The tasks are ordered by the number of workers able to perform them.  In
    this way we prioritize those tasks that few others can perform.
    """
    n = sum(map(len, who_has.values()))
    scores = {k: len(v) / n for k, v in who_has.items()}

    task_workers = {task: set.intersection(*[who_has[d] for d in data])
                          if data else set()
                    for task, data in needed.items()}
    extra = {task for task in needed if not task_workers[task]}

    worker_tasks = reverse_dict(task_workers)
    worker_tasks = {k: sorted(v, key=lambda task: len(task_workers[task]),
                                 reverse=True)
                    for k, v in worker_tasks.items()}

    return worker_tasks, extra


def reverse_dict(d):
    """

    >>> a, b, c = 'abc'
    >>> d = {'a': [1, 2], 'b': [2], 'c': []}
    >>> reverse_dict(d)  # doctest: +SKIP
    {1: {'a'}, 2: {'a', 'b'}}
    """
    result = dict((v, set()) for v in concat(d.values()))
    for k, vals in d.items():
        for val in vals:
            result[val].add(k)
    return result


@asyncio.coroutine
def handle_task(task, loop, output, reader, writer):
    msg = merge({'op': 'compute', 'reply': True}, task)
    yield from write(writer, msg)
    response = yield from read(reader)
    assert response == b'OK'
    output[task['index']] = RemoteData(task['key'], reader, writer, loop)


@asyncio.coroutine
def handle_worker(loop, tasks, shares, extra, seen, output, ident, reader=None,
        writer=None):
    if reader is None and writer is None:
        reader, writer = yield from connect(*ident, loop=loop)

    while ident in shares and shares[ident]:    # Process our own tasks
        i = shares[ident].pop()
        if i in seen:
            continue

        seen.add(i)

        yield from handle_task(tasks[i], loop, output, reader, writer)

    if ident in shares:
        del shares[ident]

    while extra:                                # Process shared tasks
        i = extra.pop()
        seen.add(i)
        yield from handle_task(tasks[i], loop, output, reader, writer)

    while shares:                               # Steal work from others
        worker = max(shares, key=lambda w: len(shares[w]))
        i = shares[ident].pop()

        if not shares[worker]:
            del shares[worker]

        if i in seen:
            continue

        seen.add(i)

        yield from handle_task(tasks[i], loop, output, reader, writer)

    return reader, writer
