from collections import OrderedDict
import re
from datum.util import dbl_quote
from psycopg2 import ProgrammingError


FIELD_TYPE_MAP = {
    'smallint':             'num',
    'bigint':               'num',
    'integer':              'num',
    'numeric':              'num',
    'double precision':     'num',
    'real':                 'num',
    'text':                 'text',
    'character varying':    'text',
    'date':                 'date',
    'timestamp without time zone': 'date',
    'USER-DEFINED':         'geom',
    'name':                 'name',
    'bytea':                'text'
}

class Table(object):
    """PostGIS table."""
    def __init__(self, parent):
        self._parent = parent
        self.db = parent.db
        self.schema = parent.schema if parent.schema not in ['', 'None', None] else 'public'
        self._c = self.db._c
        self.metadata = self._get_metadata()
        self.geom_type = self._get_geom_type() if self.geom_field else None
        self.srid = self._get_srid() if self.geom_field else None

        # Lazy cache
        self._pk_field = None

    def __str__(self):
        return f'Table: {self.name}'

    @property
    def name(self):
        return self._parent.name

    @property
    def _name_p(self):
        """The table name prepared for SQL queries."""
        name = self.name.lower()
        # Handle schema prefixes
        if '.' in name:
            return '.'.join([dbl_quote(x) for x in name.split('.')])
        else:
            return dbl_quote(name)

    def _wkt_getter(self, geom_field, to_srid=None):
        assert geom_field is not None
        geom_getter = geom_field
        if to_srid:
            geom_getter = f'ST_Transform({geom_getter}, {to_srid})'
        return f'ST_AsText({geom_getter}) AS {geom_field}'

    @property
    def count(self):
        return self._exec(f'SELECT COUNT(*) FROM {self._name_p}')[0]

    def _exec(self, stmt):
        self._c.execute(stmt)
        try:
            return self._c.fetchall()
        except ProgrammingError:
            return

    def _get_metadata(self):
        stmt = f"""
            select column_name as name, data_type as type
            from information_schema.columns
            where table_name = '{self.name}'
            and table_schema = '{self.schema}'
        """
        fields = self._exec(stmt)
        for field in fields:
            field['type'] = FIELD_TYPE_MAP[field['type']]
        return fields

    @property
    def fields(self):
        return [x['name'] for x in self.metadata]

    @property
    def non_geom_fields(self):
        return [x for x in self.fields if x != self.geom_field]

    @property
    def geom_field(self):
        f = [x for x in self.metadata if x['type'] == 'geom']
        if len(f) == 0:
            return None
        elif len(f) > 1:
            raise LookupError('Multiple geometry fields')
        return f[0]['name']

    def _get_srid(self):
        stmt = f"SELECT Find_SRID('{self.schema}', '{self.name}', '{self.geom_field}')"
        return self._exec(stmt)[0]['find_srid']

    def _get_geom_type(self):
        stmt = f"""
            SELECT type
            FROM geometry_columns
            WHERE f_table_schema = '{self.schema}'
            AND f_table_name = '{self.name}'
            and f_geometry_column = '{self.geom_field}';
        """
        # print(stmt)
        return self._exec(stmt)[0]['type']

    @property
    def pk_field(self):
        if self._pk_field is None:
            stmt = f"""
                SELECT a.attname AS name
                FROM   pg_index i
                JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                     AND a.attnum = ANY(i.indkey)
                WHERE  i.indrelid = '{self.name}'::regclass
                AND    i.indisprimary;
            """
            self._pk_field = self._exec(stmt)[0]['name']
        return self._pk_field

    def read(self, fields=None, aliases=None, geom_field=None, \
        return_geom=True, to_srid=None, limit=None, where=None, sort=None):
        """Read a DB table."""
        # Enclose table name in quotes in case there are casing issues
        table_name = self._name_p

        # default to table geom_field
        geom_field = geom_field or self.geom_field

        # Form SQL statement
        if fields:
            if aliases:
                fields = [dbl_quote(x) + (' AS {}'.format(aliases[x]) \
                    if x in aliases else '') for x in fields]
            else:
                fields = [dbl_quote(x) for x in fields]
            if geom_field and return_geom:
                wkt_getter = self._wkt_getter(geom_field, to_srid=to_srid)
                fields.append(wkt_getter)
            fields_joined = ', '.join(fields)
            stmt = f"SELECT {fields_joined} FROM {self.schema}.{table_name}"
        else:
            if geom_field and return_geom:
                wkt_getter = self._wkt_getter(geom_field, to_srid=to_srid)
                stmt = f"SELECT {table_name}.*, {wkt_getter} FROM {self.schema}.{table_name}"
            else:
                stmt = f"SELECT * FROM {self.schema}.{table_name}"
        if where:
            stmt += f" WHERE {where}"
        if sort:
            if isinstance(sort, list):
                stmt += f" ORDER BY {', '.join(sort)}"
            else:
                stmt += f" ORDER BY {sort}"

        if limit:
            stmt += f" LIMIT {limit}"
        self._c.execute(stmt)
        return self._c.fetchall()

    def delete(self, cascade=False):
        """Delete all rows."""
        name = dbl_quote(self.name)
        # RESTART IDENTITY resets sequence generators.
        stmt = f"TRUNCATE {name} RESTART IDENTITY"
        stmt += ' CASCADE' if cascade else ''
        self._c.execute(stmt)
        self.db.save()

    def _prepare_geom(self, geom, srid, transform_srid=None, multi_geom=True):
        """Prepares WKT geometry by projecting and casting as necessary."""
        geom = f"ST_GeomFromText('{geom}', {srid})"

        # Handle 3D geometries
        # TODO: screen these with regex
        if 'NaN' in geom:
            geom = geom.replace('NaN', '0')
            geom = f"ST_Force2D({geom})" # in Postgres v3 and later, name has no underscore after "Force"

        # Convert curve geometries (these aren't supported by PostGIS)
        if 'CURVE' in geom or geom.startswith('CIRC'):
            geom = f"ST_CurveToLine({geom})"
        # Reproject if necessary
        if transform_srid and srid != transform_srid:
             geom = f"ST_Transform({geom}, {transform_srid})"
        # else:
        #   geom = f"ST_GeomFromText('{geom}', {from_srid})"

        if multi_geom:
            geom = f'ST_Multi({geom})'

        return geom

    def _prepare_val(self, val, type_):
        """Prepare a value for entry into the DB."""
        if type_ == 'text':
            val = str(val) if val else ''
            # if len(val) > 0:
            val = val.replace("'", "''")    # Escape quotes
            val = "'{}'".format(val)
        elif type_ == 'num':
            if val is None:
                val = 'NULL'
            else:
                val = str(val)
        elif type_ == 'date':
            # TODO dates should be converted to real dates, not strings
            val = str(val)
        elif type_ == 'geom':
            val = str(val)
        else:
            raise TypeError(f"Unhandled type: '{type_}'")
        return val

    def _save(self):
        """Convenience method for committing changes."""
        self.db.save()

    def write(self, rows, from_srid=None, chunk_size=None):
        """
        Inserts dictionary row objects in the the database
        Args: list of row dicts, table name, ordered field names
        """
        if len(rows) == 0:
            return

        # Get fields from the row because some fields from self.fields may be
        # optional, such as autoincrementing integers.
        fields = rows[0].keys()
        geom_field = self.geom_field
        srid = from_srid or self.srid
        row_geom_type = re.match('[A-Z]+', rows[0][geom_field]).group() \
            if geom_field else None
        table_geom_type = self.geom_type if geom_field else None

        # Do we need to cast the geometry to a MULTI type? (Assuming all rows
        # have the same geom type.)
        if geom_field:
            if self.geom_type.startswith('MULTI') and \
                not row_geom_type.startswith('MULTI'):
                multi_geom = True
            else:
                multi_geom = False

        # Make a map of non geom field name => type
        type_map = OrderedDict()
        for field in fields:
            try:
                type_map[field] = [x['type'] for x in self.metadata if x['name'] == field][0]
            except IndexError:
                raise ValueError(f'Field `{field}` does not exist')
        type_map_items = type_map.items()

        fields_joined = ', '.join(fields)
        stmt = f"INSERT INTO {self.name} ({fields_joined}) VALUES "

        len_rows = len(rows)
        if chunk_size is None or len_rows < chunk_size:
            iterations = 1
        else:
            iterations = int(len_rows / chunk_size)
            iterations += (len_rows % chunk_size > 0)  # round up

        # Make list of value lists
        for i in range(0, iterations):
            val_rows = []
            cur_stmt = stmt
            if chunk_size:
                start = i * chunk_size
                end = min(len_rows, start + chunk_size)
            else:
                start = i
                end = len_rows

            for row in rows[start:end]:
                val_row = []
                for field, type_ in type_map_items:
                    if type_ == 'geom':
                        geom = row[geom_field]
                        val = self._prepare_geom(geom, srid, multi_geom=multi_geom)
                        val_row.append(val)

                    else:
                        val = self._prepare_val(row[field], type_)
                        val_row.append(val)
                val_rows.append(val_row)

            # Execute
            vals_joined = [f"({', '.join(vals)})" for vals in val_rows]
            rows_joined = ', '.join(vals_joined)
            cur_stmt += rows_joined
            self._c.execute(cur_stmt)
            self._save()


    """INDEXES"""

    def _name_for_index(self, fields):
        """This is approximately what Postgres will suggest for index names."""
        comps = [self.name] + list(fields) + ['idx']
        return '_'.join(comps)

    def create_index(self, *fields, **kwargs):
        name = kwargs.get('name') or self._name_for_index(fields)
        comps = [
            'CREATE INDEX IF NOT EXISTS',
            name,
            'ON',
            self.name,
            f"({', '.join(fields)})"
        ]
        stmt = ' '.join(comps)
        self._exec(stmt)
        self.db.save()

    def drop_index(self, *fields, **kwargs):
        '''
        Drops an index by name, if it exists
        '''
        name = kwargs.get('name') or self._name_for_index(fields)
        stmt = f"DROP INDEX IF EXISTS {name}"
        self._exec(stmt)
        self.db.save()
