from .database import Database
# from .table import Table

def connect(url):
    db = Database(url)
    return db
