from .util import parse_url
from .table import Table
from .postgis import Database as PostgisDatabase
from .oracle_stgeom import Database as OracleStgeomDatabase

# This translates DB schemes to adapter-specific classes.
CLASS_MAP = {
    'postgis':          PostgisDatabase,
    'oracle-stgeom':    OracleStgeomDatabase,
}

class Database(object):
    """Proxy class for database adapters."""
    def __init__(self, url):
        self.url = url
        scheme = self.scheme = parse_url(url)['scheme']
        if scheme not in CLASS_MAP:
            raise ValueError('Unknown database type: {}'.format(scheme))
        _ChildDatabase = CLASS_MAP[scheme]
        self._child = _ChildDatabase(self)

    def __str__(self):
        fmt = 'Database: {scheme}://{user}:***@{host}'
        fmt += '/' + self.name if self.name else ''
        return fmt.format(**self._child.__dict__)

    def __getitem__(self, key):
        """Alternate notation for getting a table: db['table']"""
        return self.table(key)

    @property
    def name(self):
        return self._child.name

    @property
    def user(self):
        return self._child.user

    @property
    def _c(self):
        return self._child._c

    def execute(self, stmt):
        return self._child.execute(stmt)

    def save(self):
        self._child.save()

    def close(self):
        self._child.close()

    def table(self, name):
        """Get a reference to a database table"""
        return Table(self, name)

    @property
    def tables(self):
        """Get a list of all table names."""
        return self._child.tables

    @property
    def count(self):
        """Count rows."""
        return self._child.count

    def create_table(self, table, fields):
        self._child.create_table(table, fields)

    def drop_table(self, table):
        self._child.drop_table(table)

    def create_view(self, view, select_stmt):
        self._child.create_view(view, select_stmt)

    def drop_view(self, view):
        self._child.drop_view(view)
