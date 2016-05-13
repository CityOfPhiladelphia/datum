import re
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

class Table(object):
    """Oracle ST_Geometry table."""
    def __init__(self, parent):
        self.parent = parent
        self.db = parent.db
        self._c = self.db._c
        self.metadata = self._get_metadata()
        self.geom_field = self._get_geom_field()
        self.geom_type = self._get_geom_type() if self.geom_field else None
        self.srid = self._get_srid() if self.geom_field else None

    # def _prepare_table_name(self, name):
    #     name = name.upper()
    #     # Handle schema prefixes
    #     if '.' in name:
    #         return '.'.join([dbl_quote(x) for x in name.split('.')])
    #     else:
    #         return dbl_quote(self.name)

    @property
    def name(self):
        return self.parent.name

    @property
    def _name_p(self):
        name = self.name.upper()
        # Handle schema prefixes
        if '.' in name:
            return '.'.join([dbl_quote(x) for x in name.split('.')])
        else:
            return dbl_quote(name)

    def _exec(self, stmt):
        self._c.execute(stmt)
        return self._c.fetchall()

    @property
    def fields(self):
        return [x['name'].lower() for x in self.metadata]

    def _get_srid(self):
        stmt = "SELECT SDE.ST_SRID({0.geom_field}) FROM {0._name_p} WHERE \
            ROWNUM = 1".format(self)
        self._c.execute(stmt)
        row = self._c.fetchone()
        # An empty table won't return anything
        if row is None:
            return row
        return row[0]

    def _get_geom_type(self):
        stmt = "SELECT SDE.ST_GeometryType({}) FROM {} WHERE ROWNUM = 1"\
            .format(self.geom_field, self._name_p)
        row = self._exec(stmt)
        # ST_GeometryType returns nothing if the table is empty, so don't try
        # to unpack the value.
        if len(row) < 1:
            return None
        return self._exec(stmt)[0][0].replace('ST_', '')

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
        if geom_field and ' M ' in rows[0][geom_field_i]:
            scrub_m_geom_type_re = re.compile(self.geom_type + ' M')
            scrub_m_value_re = re.compile(' 1.#QNAN000')
            for row in rows:
                geom = row[geom_field_i]
                geom = scrub_m_geom_type_re.sub(self.geom_type, geom)
                geom = scrub_m_value_re.sub('', geom)
                row[geom_field_i] = geom
        
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
        geom = "ST_GeomFromText('{}', {})".format(geom, srid)

        # Handle 3D geometries
        # TODO: screen these with regex
        if 'NaN' in geom:
            geom = geom.replace('NaN', '0')
            geom = "ST_Force_2D({})".format(geom)

        # Convert curve geometries (these aren't supported by PostGIS)
        if 'CURVE' in geom or geom.startswith('CIRC'):
            geom = "ST_CurveToLine({})".format(geom)
        # Reproject if necessary
        if transform_srid and srid != transform_srid:
             geom = "ST_Transform({}, {})".format(geom, transform_srid)
        # else:
        #   geom = "ST_GeomFromText('{}', {})".format(geom, from_srid)

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
            if len(val) > 0:
                val = val.replace("'", "''")    # Escape quotes
                val = "'{}'".format(val)
            else:
                val = "''"
        elif type_ == 'num':
            pass
        elif type_ == 'geom':
            pass
        else:
            raise TypeError("Unhandled type: '{}'".format(type_))
        return val

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
                print(self.metadata)
                raise ValueError('Field `{}` does not exist'.format(field))
        type_map_items = type_map.items()

        # Prepare cursor for many inserts
        fields_joined = ', '.join(fields)
        placeholders = ', '.join(':' + x for x in fields)
        stmt = "INSERT INTO {} ({}) VALUES ({})".format(self.name, \
            fields_joined, placeholders)
        self._c.prepare(stmt)

        len_rows = len(rows)
        if chunk_size is None or len_rows < chunk_size:
            iterations = 1
        else:
            iterations = int(len_rows / chunk_size)
            iterations += (len_rows % chunk_size > 0)  # round up

        # Make list of value lists
        for i in range(0, iterations):
            val_rows = []
            # cur_stmt = stmt
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
            # vals_joined = ['({})'.format(', '.join(vals)) for vals in val_rows]
            # rows_joined = ', '.join(vals_joined)
            # cur_stmt += rows_joined
            print(val_rows)
            self._c.executemany(None, val_rows)
            self._save()
            
    def _save(self):
        """Convenience method for committing changes."""
        self.db.save()