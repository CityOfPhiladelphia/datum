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

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)