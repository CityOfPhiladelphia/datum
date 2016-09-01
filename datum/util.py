from functools import partial
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
