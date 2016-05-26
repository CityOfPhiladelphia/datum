from .database import Database

def connect(url):
    # TODO this should support things other than databases, like CSV sheets.
    return Database(url)

def db(url):
    return Database(url)
