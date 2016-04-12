import re
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

    @property
    def non_geom_fields(self):
        return [x for x in self.fields if x != self.geom_field]

    def _get_srid(self):
        stmt = "SELECT SDE.ST_SRID({0.geom_field}) FROM {0._name_p} WHERE \
            ROWNUM = 1".format(self)
        self._c.execute(stmt)
        return self._c.fetchone()[0]

    def _get_geom_type(self):
        stmt = "SELECT SDE.ST_GeometryType({}) FROM {} WHERE ROWNUM = 1"\
            .format(self.geom_field, self._name_p)
        return self._exec(stmt)[0][0].replace('ST_', '')

    def _get_metadata(self):
        stmt = "SELECT * FROM {} WHERE 1 = 0".format(self._name_p)
        self._c.execute(stmt)
        desc = self._c.description
        fields = []
        for field in desc:
            name = field[0]
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
        return [x['name'] for x in self.metadata if x['type'] != 'geom']

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
                geom = row[geom_field_i].read()
                row[geom_field_i] = geom
            rows.append(row)


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
