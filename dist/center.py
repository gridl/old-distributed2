import asyncio

from .core import read, write

@asyncio.coroutine
def manage_metadata(who_has, has_what, reader, writer, msg):
    if msg['op'] == 'register':
        has_what[msg['address']].update(msg['keys'])
        for key in msg['keys']:
            who_has[key].add(msg['address'])
        if msg.get('reply'):
            yield from write(writer, b'OK')

    if msg['op'] == 'unregister':
        for key in msg['keys']:
            if key in has_what[msg['address']]:
                has_what[msg['address']].remove(key)
            try:
                who_has[key].remove(msg['address'])
            except KeyError:
                pass
        if msg.get('reply'):
            yield from write(writer, b'OK')

    if msg['op'] == 'who-has':
        result = {k: who_has[k] for k in msg['keys']}
        yield from write(writer, result)

    if msg['op'] == 'has-what':
        result = {k: has_what[k] for k in msg['keys']}
        yield from write(writer, result)
