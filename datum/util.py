from functools import partial
from itertools import islice, chain
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
    """
    Return chunks of a max size of the iterable
    Thanks to http://stackoverflow.com/a/24527424
    """
    if not size:
        raise ValueError('Size must be an integer greater than 0')

    try:
        iterator = iter(iterable)
    except TypeError:
        raise TypeError('First argument must be an iterable object, not {}'
                        .format(type(iterable).__name__))

    # Pull a single element off of the iterator to ensure
    # that it is not empty. This will stop looping when the
    # iterator is depleted.
    for first in iterator:
        # Yield an iterable filled out with the apprpriate
        # number of remaining elements from the iterator.
        yield chain([first], islice(iterator, size - 1))

def isiterable(obj):
    try:
        iter(obj)
    except TypeError:
        return False
    else:
        return True
