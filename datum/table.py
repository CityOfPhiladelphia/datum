from datum.postgis import Table as PostgisTable
from datum.oracle_stgeom import Table as OracleStgeomTable

CLASS_MAP = {
    'postgis':          PostgisTable,
    'oracle-stgeom':    OracleStgeomTable,
}

class Table(object):
    """Proxy class for tables."""
    def __init__(self, db, name):
        self.db = db
        self.name = name

        _ChildTable = CLASS_MAP[db.scheme]
        self._child = _ChildTable(self)

    def __str__(self):
        return 'Table: {}'.format(self.name)

    @property
    def pk_field(self):
        """Return the primary key field."""
        return self._child.pk_field

    @property
    def geom_field(self):
        """Returns the name of the geometry field."""
        return self._child.geom_field

    @property
    def geom_type(self):
        """Returns the OGC geometry type (e.g. LINESTRING, MULTIPOLYGON)."""
        return self._child.geom_type

    @property
    def metadata(self):
        """Returns a list of field attribute dictionaries."""
        return self._child.metadata

    @property
    def count(self):
        return self._child.count

    @property
    def fields(self):
        """Returns a list of field names."""
        return self._child.fields

    def read(self, fields=None, geom_field=None, to_srid=None, limit=None, \
        where=None, sort=None):
        """
        Read rows from the database.
        
        ```
        Parameters
        ----------
        fields : list, optional
        geom_field : str, optional
        to_srid : int, optional
        limit : int, optional
        where : str, optional
        sort : str, optional
        """
        return self._child.read(fields=fields, geom_field=geom_field, \
            to_srid=to_srid, limit=limit, where=where, sort=sort)

    def write(self, rows, from_srid=None, chunk_size=None):
        self._child.write(rows, from_srid=from_srid, chunk_size=chunk_size)

    def delete(self, cascade=False):
        """Delete all rows."""
        return self._child.delete(cascade=cascade)


    """INDEXES"""

    def create_index(self, field):
        self._child.create_index(field)

    def drop_index(self, field):
        self._child.drop_index(field)
