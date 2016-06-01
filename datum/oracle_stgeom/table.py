import re
from datetime import datetime
from collections import OrderedDict
from datum.util import dbl_quote, WktTransformer
from cx_Oracle import OBJECT as CxOracleObject
import cx_Oracle


# These are strings because one type (OBJECTVAR) isn't importable from 
# the cx_Oracle module.
FIELD_TYPE_MAP = {
    'NUMBER':       'num',
    'NCHAR':        'text',
    'STRING':       'text',
    'DATETIME':     'date',
    'FIXED_CHAR':   'text',
    # HACK: Nothing else in an SDE database should be using OBJECTVAR.
    'OBJECTVAR':    'geom',
    # Not sure why cx_Oracle returns this for a NUMBER field.
    'LONG_STRING':  'num',
}
m_geom_type_re = re.compile(' M(?= )')
m_value_re = re.compile(' 1.#QNAN000')

class Table(object):
    """Oracle ST_Geometry table."""
    def __init__(self, parent):
        self.parent = parent
        self.db = parent.db
        self.name = parent.name
        self.schema = parent.schema
        self._c = self.db._c
        self.metadata = self._get_metadata()
        self.geom_field = self._get_geom_field()
        self.geom_type = self._get_geom_type() if self.geom_field else None
        self.srid = self._get_srid() if self.geom_field else None
        self.objectid_field = self._get_objectid_field()

    @property
    def _name_p(self):
        """Returns the table name prepended with the schema name, prepared for
        a query."""

        # If there's a schema we have to double quote the owner and the table
        # name, but also make them uppercase.
        if self.schema:
            comps = [self.schema.upper(), self.name.upper()]
            return '.'.join([dbl_quote(x) for x in comps])
        return self.name

    @property
    def _owner(self):
        """Return the owner name for querying system tables. This is either
        the schema or the DB user."""
        return self.schema or self.db.user

    def _exec(self, stmt):
        self._c.execute(stmt)
        return self._c.fetchall()

    @property
    def fields(self):
        return [x['name'] for x in self.metadata]

    def _get_srid(self):
        stmt = '''
            select s.auth_srid
            from sde.layers l
            join sde.spatial_references s
            on l.srid = s.srid
            where l.owner = '{}' and l.table_name = '{}'
        '''.format(self._owner.upper(), self.name.upper())
        self._c.execute(stmt)
        row = self._c.fetchone()
        return row[0]

    def _get_geom_type(self):
        """
        Returns the OGC geometry type for a table.

        This is complicated because SDE.ST_GeomType doesn't return anything
        when the table is empty. As a workaround, inspect the bitmasked values
        of the EFLAGS column of SDE.LAYERS to guess what geom type was
        specified at time of creation. Sometimes, however, the multipart flag
        is set to true but the actual geometries are stored as single-part.
        For now, any geometry in a multipart-enabled column will be returned as
        MULTIxxx (see `read` method for more).

        Shout-out to Vince as usual:
        http://gis.stackexchange.com/questions/193424/get-the-geometry-type-of-an-empty-arcsde-feature-class
        """
        stmt = '''
            select
                bitand(eflags, 2),
                bitand(eflags, 4) + bitand(eflags, 8),
                bitand(eflags, 16),
                bitand(eflags, 262144) 
            from sde.layers
            where
                owner = '{}' and
                table_name = '{}'
        '''.format(self._owner.upper(), self.name.upper())
        point, line, polygon, multipart = self._exec(stmt)[0]
        if point > 0:
            geom_type = 'POINT'
        elif line > 0:
            geom_type = 'LINESTRING'
        elif polygon > 0:
            geom_type = 'POLYGON'
        else:
            raise ValueError('Unknown geometry type')
        if multipart > 0:
            geom_type = 'MULTI' + geom_type
        return geom_type

    def _get_metadata(self):
        stmt = "SELECT * FROM {} WHERE 1 = 0".format(self._name_p)
        self._c.execute(stmt)
        desc = self._c.description
        fields = []
        for field in desc:
            name = field[0].lower()
            type_ = field[1].__name__
            assert type_ in FIELD_TYPE_MAP, '{} not a known field type'\
                .format(type_)

            fields.append({
                'name':     name,
                'type':     FIELD_TYPE_MAP[type_],
            })
        return fields

    def _get_objectid_field(self):
        """Get the object ID field with a not-null constraint."""
        stmt = '''
            SELECT
                LOWER(COLUMN_NAME)
            FROM
                ALL_TAB_COLS
            WHERE
                UPPER(OWNER) = UPPER('{schema}') AND
                UPPER(TABLE_NAME) = UPPER('{name}') AND
                NULLABLE = 'N' AND
                COLUMN_NAME LIKE 'OBJECTID%'
        '''.format(schema=self._owner, name=self.name)
        fields = self._exec(stmt)
        assert len(fields) == 1 and len(fields[0]) == 1 and \
            'Could not get OBJECTID field for {}'.format(self.name)
        # This should never happen, but assert it anyway to be clear.
        return fields[0][0]

    # @property
    # def _geom_field_i(self):
    #     """Get the index of the geometry field."""
    #     assert self.geom_field
    #     return self.fields.index(self.geom_field)

    def _get_geom_field(self):
        f = [x for x in self.metadata if x['type'] == 'geom']
        if len(f) == 0:
            return None
        elif len(f) > 1:
            raise LookupError('Multiple geometry fields')
        return f[0]['name'].lower()

    @property
    def non_geom_fields(self):
        # return [x['name'].lower() for x in self.metadata if x['type'] != 'geom']
        return [x for x in self.fields if x != self.geom_field]

    def _get_wkt_selector(self, to_srid=None):
        assert self.geom_field
        geom_field_t = geom_field = self.geom_field
        # SDE.ST_Transform doesn't work when the datums differ. Unfortunately, 
        # 4326 <=> 2272 is one of those. Using Shapely + PyProj for now.
        # if to_srid and to_srid != self.srid:
        #     geom_field_t = "SDE.ST_Transform({}, {})"\
        #         .format(geom_field, to_srid)
        return "SDE.ST_AsText({}) AS {}"\
            .format(geom_field_t, geom_field)

    def _has_m_value(self, wkt):
        """Checks a WKT geometry for an m-value (used in linear referencing.)"""
        return (m_geom_type_re.search(wkt) is not None)

    def _remove_m_value(self, wkt):
        """
        Removes the m-value from a WKT geometry.
        TODO: do this more elegantly/generically.
        """
        # Take the `M` out
        wkt = m_geom_type_re.sub('', wkt)
        # Take the  1.#QNAN000 out.
        wkt = m_value_re.sub('', wkt)
        return wkt

    def read(self, fields=None, aliases=None, geom_field=None, to_srid=None,
        return_geom=True, limit=None, where=None, sort=None):
        # If no geom_field was specified and we're supposed to return geom, 
        # get it from the object.
        geom_field = geom_field or (self.geom_field if return_geom else None)

        # Select
        fields = fields or self.non_geom_fields
        select_items = list(fields)
        if return_geom:
            if geom_field:
                select_items.append(self._get_wkt_selector(to_srid=to_srid))
                fields.append(geom_field)
            # else:
            #     raise ValueError('No geometry field to select')
        joined = ', '.join(select_items)
        stmt = "SELECT {} FROM {}".format(joined, self._name_p)

        # Other params
        if where:
            stmt += " WHERE {}".format(where)
            if limit:
                stmt += " AND ROWNUM <= {}".format(limit)
        elif limit:
            stmt += " WHERE ROWNUM <= {}".format(limit)

        self._c.execute(stmt)
        
        # Handle aliases
        # fields = [re.sub('.+ AS ', '', x, flags=re.IGNORECASE) for x in fields]
        if aliases:
          fields = [aliases[x] if x in aliases else x for x in fields]

        fields_lower = [x.lower() for x in fields] 
        if geom_field:
            geom_field_i = fields.index(geom_field)
        rows = []

        # Unpack geometry.
        for source_row in self._c:
            row = list(source_row)
            if geom_field:
                row[geom_field_i] = row[geom_field_i].read()
            rows.append(row)

        # If there were no rows returned, don't move on to next step where
        # we try to get a row.
        if len(rows) == 0:
            return rows

        # Check if we need to scrub m-values.
        # WKT will look like `POLYGON M (...)`
        if geom_field:
            if self._has_m_value(rows[0][geom_field_i]):
                for row in rows:
                    geom = self._remove_m_value(row[geom_field_i])
                    row[geom_field_i] = geom
        
            # TODO if the WKT geom is single but the geom_type for the table
            # is multi, we may want to convert it. Seems to be working for now
            # though.

        # Dictify.
        rows = [dict(zip(fields_lower, row)) for row in rows]

        # Transform if we need to
        if to_srid and to_srid != self.srid:
            geom_field_l = geom_field.lower()
            tsf = WktTransformer(self.srid, to_srid)
            for row in rows:
                geom = row[geom_field_l]
                geom_t = tsf.transform(geom)
                row[geom_field_l] = geom_t        

        return rows

    def _prepare_geom(self, geom, srid, transform_srid=None, multi_geom=True):
        """Prepares WKT geometry by projecting and casting as necessary."""

        if geom is None:
            # TODO: should this use the `EMPTY` keyword?
            return '{} EMPTY'.format(self.geom_type)

        # Uncomment this to use write method #1 (see write function for details)
        # geom = "SDE.ST_Geometry('{}', {})".format(geom, srid)

        # Handle 3D geometries
        # TODO screen these with regex
        if 'NaN' in geom:
            geom = geom.replace('NaN', '0')
            geom = "ST_Force_2D({})".format(geom)

        # TODO this was copied over from PostGIS, but maybe Oracle can handle
        # them as-is?
        if 'CURVE' in geom or geom.startswith('CIRC'):
            geom = "ST_CurveToLine({})".format(geom)
        # Reproject if necessary
        # TODO: do this with pyproj since ST_Geometry can't
        # if transform_srid and srid != transform_srid:
        #      geom = "ST_Transform({}, {})".format(geom, transform_srid)

        if multi_geom:
            geom = 'ST_Multi({})'.format(geom)

        return geom

    def _prepare_val(self, val, type_):
        """Prepare a value for entry into the DB."""
        if val is None:
            return 'NULL'

        # Make all vals strings for inserting into SQL statement.
        val = str(val)

        if type_ == 'text':
            pass
            # With executemany we don't need this
            # if len(val) > 0:
            #     val = val.replace("'", "''")    # Escape quotes
            #     # val = "'{}'".format(val)
            # else:
            #     val = "''"
        elif type_ == 'num':
            pass
        elif type_ == 'geom':
            pass
        elif type_ == 'date':
            # Convert datetimes to ISO-8601
            if isinstance(val, datetime):
                val = val.isoformat()
        else:
            raise TypeError("Unhandled type: '{}'".format(type_))
        return val

    def write(self, rows, from_srid=None, chunk_size=None):
        """
        Inserts dictionary row objects in the the database.
        Args: list of row dicts, table name, ordered field names

        Originally this formed one big INSERT statement with a chunks of x
        rows, but it's considerably faster to use the cx_Oracle `executemany` 
        function. See methods 1 and 2 below.

        TODO: it might be faster to call NEXTVAL on the DB sequence for OBJECTID
        rather than use the SDE helper function.
        """
        if len(rows) == 0:
            return

        # Get fields from the row because some fields from self.fields may be
        # optional, such as autoincrementing integers.
        fields = rows[0].keys()
        geom_field = self.geom_field
        geom_type = self.geom_type
        srid = from_srid or self.srid
        table_geom_type = self.geom_type if geom_field else None
        # row_geom_type = re.match('[A-Z]+', rows[0][geom_field]).group() \
        #     if geom_field else None

        # Look for an insert row with a geom to get the geom type.
        row_geom_type = None
        for row in rows:
            geom = row[geom_field]
            if geom:
                row_geom_type = re.match('[A-Z]+', geom).group()
                break

        # Do we need to cast the geometry to a MULTI type? (Assuming all rows 
        # have the same geom type.)
        if geom_field:
            # Check for a geom_type first, in case the table is empty.
            if geom_type and geom_type.startswith('MULTI') and \
                not row_geom_type.startswith('MULTI'):
                multi_geom = True
            else:
                multi_geom = False

        # Make a map of non geom field name => type
        type_map = OrderedDict()
        for field in fields:
            try:
                type_map[field] = [x['type'] for x in self.metadata if \
                    x['name'] == field][0]
            except IndexError:
                raise ValueError('Field `{}` does not exist'.format(field))
        type_map_items = type_map.items()

        # Prepare cursor for many inserts

        # # METHOD 1: one big SQL statement. Note you also have to uncomment a
        # # line in _prepare_geom to make this work.
        # # In Oracle this looks like:
        # # INSERT ALL
        # #    INTO t (col1, col2, col3) VALUES ('val1_1', 'val1_2', 'val1_3')
        # #    INTO t (col1, col2, col3) VALUES ('val2_1', 'val2_2', 'val2_3')
        # # SELECT 1 FROM DUAL;
        # fields_joined = ', '.join(fields)
        # stmt = "INSERT ALL {} SELECT 1 FROM DUAL"
        
        # # We always have to pass in a value for OBJECTID (or whatever the SDE
        # # PK field is; sometimes it's something like OBJECTID_3). Check to see
        # # if the user passed in a value for object ID (not likely), otherwise 
        # # hardcode the sequence incrementor into the prepared statement.
        # if self.objectid_field in fields:
        #     into_clause = "INTO {} ({}) VALUES ({{}})".format(self.name, \
        #         fields_joined)
        # else:
        #     incrementor = "SDE.GDB_UTIL.NEXT_ROWID('{}', '{}')".format(self._owner, self.name)
        #     into_clause = "INTO {} ({}, {}) VALUES ({{}}, {})".format(self.name, fields_joined, self.objectid_field, incrementor)
        
        # METHOD 2: executemany (not working with SDE.ST_Geometry call)
        placeholders = []
        stmt_fields = list(fields)
        # Create placeholders for prepared statement
        for field in fields:
            type_ = type_map[field]
            if type_ == 'geom':
                placeholders.append('SDE.ST_Geometry(:{}, {})'\
                    .format(field, self.srid))
            elif type_ == 'date':
                # Insert an ISO-8601 timestring
                placeholders.append("TO_TIMESTAMP(:{}, 'YYYY-MM-DD\"T\"HH24:MI:SS\"+00:00\"')".format(field))
            else:
                placeholders.append(':' + field)
        # Inject the object ID field if it's missing from the supplied rows
        if self.objectid_field not in fields:
            stmt_fields.append(self.objectid_field)
            incrementor = "SDE.GDB_UTIL.NEXT_ROWID('{}', '{}')"\
                .format(self._owner, self.name)
            placeholders.append(incrementor)
        # Prepare statement
        placeholders_joined = ', '.join(placeholders)
        stmt_fields_joined = ', '.join(stmt_fields)
        stmt = "INSERT INTO {} ({}) VALUES ({})".format(self.name, \
            stmt_fields_joined, placeholders_joined)
        self._c.prepare(stmt)

        # END OF METHODS
        
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
                        val = self._prepare_geom(geom, srid, \
                            multi_geom=multi_geom)
                        val_row.append(val)
                    else:
                        val = self._prepare_val(row[field], type_)
                        val_row.append(val)
                val_rows.append(val_row)

            # Execute
            # # METHOD 1
            # vals_joined = [', '.join(vals) for vals in val_rows]
            # cur_into_clauses = ' '.join([into_clause.format(x) for x in vals_joined])
            # cur_stmt = cur_stmt.format(cur_into_clauses)
            # self._c.execute(cur_stmt)
            # self._save()

            # METHOD 2
            self._c.executemany(None, val_rows)
            self._save()

    def _save(self):
        """Convenience method for committing changes."""
        self.db.save()