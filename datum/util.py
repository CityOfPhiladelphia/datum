from functools import partial
try:
    from itertools import izip_longest as zip_longest
except ImportError:
    from itertools import zip_longest
from six.moves.urllib.parse import urlparse

def dbl_quote(text):
    """Place double quotes around a string."""
    return '"{}"'.format(text)

def parse_url(url):
    p = urlparse(url)
    comps = {
        'scheme':       p.scheme,
        'host':         p.hostname,
        'user':         p.username,
        'password':     p.password,
        'db_name':      p.path[1:] if p.path else None,
    }
    return comps

def chunks_of(iterable, size):
    """Return chunks of a max size of the iterable"""
    def get_chunk(iterator, size):
        for _ in range(size):
            yield next(iterator)

    iterator = iter(iterable)
    while True:
        yield get_chunk(iterator, size)

def isiterable(obj):
    try:
        iter(obj)
    except TypeError:
        return False
    else:
        return True
