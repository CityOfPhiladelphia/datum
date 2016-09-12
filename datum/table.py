from datum.postgis import Table as PostgisTable
from datum.oracle_stgeom import Table as OracleStgeomTable

TABLE_CLASS_MAP = {
    'postgis':          PostgisTable,
    'oracle-stgeom':    OracleStgeomTable,
}

class Table(object):
    """Proxy class for tables."""
    def __init__(self, db, name):
        self.db = db

        # Check for a schema
        name = name
        if '.' in name:
            comps = name.split('.')
            self.schema = comps[0]
            self.name = comps[1]
        else:
            self.schema = None
            self.name = name

        _ChildTable = TABLE_CLASS_MAP[db.adapter]
        self._child = _ChildTable(self)

    def __str__(self):
        if self.schema:
            str_ = '.'.join([self.schema, self.name])
        else:
            str_ = self.name
        return 'Table: {}'.format(str_)

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
    def non_geom_fields(self):
        """Returns all non-geometry fields."""
        return self._child.non_geom_fields

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

    def load(self, *infiles, from_srid=None, chunk_size=None):
        import csv

        if len(infiles) == 0:
            infiles = (sys.stdin,)

        for infile in infiles:
            reader = csv.DictReader(infile)
            self.write(list(reader), from_srid=from_srid, chunk_size=chunk_size)

    def read(self, fields=None, aliases=None, geom_field=None, to_srid=None, \
        return_geom=True, limit=None, where=None, sort=None):
        """
        Read rows from the database.
        
        ```
        Parameters
        ----------
        fields : list, optional
        aliases: dict, optional
        geom_field : str, optional
        to_srid : int, optional
        limit : int, optional
        where : str, optional
        sort : str, optional
        """
        return self._child.read(fields=fields, aliases=aliases, \
            geom_field=geom_field, return_geom=return_geom, to_srid=to_srid, \
            limit=limit, where=where, sort=sort)

    def write(self, rows, from_srid=None, chunk_size=None):
        self._child.write(rows, from_srid=from_srid, chunk_size=chunk_size)

    def delete(self, cascade=False):
        """Delete all rows."""
        return self._child.delete(cascade=cascade)


    """INDEXES"""

    def create_index(self, *fields, **kwargs):
        name = kwargs.get('name')
        self._child.create_index(*fields, name=name)

    def drop_index(self, *fields, **kwargs):
        name = kwargs.get('name')
        self._child.drop_index(*fields, name=name)
