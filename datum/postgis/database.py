from datum.util import parse_url
# from . import Table
import psycopg2
from psycopg2.extras import RealDictCursor


class Database(object):
    """Wrapper for a PostGIS database."""


    """GENERAL"""

    def __init__(self, parent):
        url = parent.url
        self.parent = parent
        p = parse_url(url)
        self.scheme = p['scheme']
        self.host = p['host']
        self.user = p['user']
        self.password = p['password']
        self.name = p['db_name']

        # Cache these, but lazy load.
        self._tables = None

        # Format these for psycopg2.
        params = {
           'database':  self.name,
           'user':      self.user,
           'password':  self.password,
           'host':      self.host, 
        }
        self._cxn = psycopg2.connect(**params)
        self._c = self._cxn.cursor(cursor_factory=RealDictCursor)

    def close(self):
        self._cxn.close()

    def save(self):
        """Commit database changes."""
        self._cxn.commit()

    def execute(self, stmt):
        """Execute a SQL statement and return all rows."""
        self._c.execute(stmt)
        try:
            rows = self._c.fetchall()
        # We already executed the statement, so if there's a ProgrammingError
        # here it's probably just that no rows were returned.
        except psycopg2.ProgrammingError:
            return None
        # Unpack single values
        if len(rows) > 0 and len(rows[0]) == 1:
            rows = [x[0] for x in rows]
        return rows


    """TABLES"""

    @property
    def tables(self):
        if self._tables is None:
            self._tables = self._get_tables()
        return self._tables

    def _get_tables(self):
        tables = self.table('information_schema.tables').read(where=\
            "table_schema = 'public' AND table_type = 'BASE TABLE'")
        return [x['table_name'] for x in tables]

    def table(self, name):
        # return Table(self, name)
        return self.parent.table(name)

    def create_table(self, name, cols):
        '''
        Creates a table if it doesn't already exist.

        Args: table name and a list of column dictionaries like:
            name:   my_table
            type:   integer
        '''
        
        # Make concatenated string of columns, datatypes
        col_string_list = ['id serial']
        col_string_list += ['{} {}'.format(x['name'], x['type']) for x in cols]
        col_string_list.append('PRIMARY KEY(id)')
        col_string = ', '.join(col_string_list)

        stmt = 'CREATE TABLE IF NOT EXISTS {} ({})'.format(name, col_string)
        self._c.execute(stmt)
        self.save()

    def drop_table(self, name):
        stmt = 'DROP TABLE IF EXISTS {}'.format(name)
        self._c.execute(stmt)
        self.save()


    """VIEWS"""

    def create_view(self, view, select_stmt):
        stmt = "CREATE VIEW {} AS {}".format(view, select_stmt)
        self._c.execute(stmt)
        self.save()

    def drop_view(self, view):
        stmt = "DROP VIEW IF EXISTS {}".format(view)
        self._c.execute(stmt)
        self.save()

    def create_mview(self, mview, select_stmt):
        stmt = "CREATE MATERIALIZED VIEW {} AS {}".format(mview, select_stmt)
        self._c.execute(stmt)
        self.save()

    def drop_mview(self, mview):
        stmt = "DROP MATERIALIZED VIEW IF EXISTS {}".format(mview)
        self._c.execute(stmt)
        self.save()
