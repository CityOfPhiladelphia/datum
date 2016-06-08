import os
import cx_Oracle
from datum.util import parse_url
# from .table import Table


class Database(object):
    """Oracle database connection."""

    def __init__(self, parent):
        self.parent = parent
        url = parent.url
        p = parse_url(url)
        self.host = p['host']
        self.user = p['user'].lower()
        self.password = p['password']
        self.name = p['db_name']
        if self.name: self.name = self.name.lower()
        
        dsn = '{user}/{password}@{host}'.format(**self.__dict__)
        if self.name: dsn += '/' + self.name

        # Prevent cx_Oracle from converting everything to ASCII.
        os.environ['NLS_LANG'] = '.UTF8'

        self.cxn = cx_Oracle.connect(dsn)
        self._c = self.cxn.cursor()

    def execute(self, stmt):
        self._c.execute(stmt)
        try:
            rows = self._c.fetchall()
        # Return rowcount for non-SELECT operations
        except cx_Oracle.InterfaceError:
            return self._c.rowcount
        # Unpack single values
        if len(rows) > 0 and len(rows[0]) == 1:
            rows = [x[0] for x in rows]
        return rows

    def close(self):
        self.cxn.close()

    # def table(self, name):
    #     # Check for a schema
    #     if '.' in 
    #     return self.parent.table(name)

    @property
    def _user_p(self):
        return self.user.upper()

    @property
    def tables(self):
        stmt = """
            SELECT TABLE_NAME FROM USER_TABLES
            WHERE TABLE_NAME NOT IN (
                SELECT VIEW_NAME
                FROM ALL_VIEWS
                WHERE OWNER = '{}')
        """.format(self._user_p)
        return sorted(self.execute(stmt))

    ############################################################################
    # READ
    ############################################################################

    def _dictify(self, geom_field=None):
        '''
        Turns query results into a list of dictionaries. This reads from the 
        cursor because calling fetchall() on rows breaks the geometry LOB.
        '''
        fields = [x[0].lower() for x in self._c.description]
        rows = self._c

        # Non-spatial
        if geom_field is None:
            return [dict(zip(fields, row)) for row in rows]

        # Spatial
        else:
            non_geom_fields = [x for x in fields if x != geom_field]
            wkt_field = geom_field + '_wkt'
            wkt_field_i = non_geom_fields.index(wkt_field)
            dicts = []
            for row in rows:
                the_dict = dict(zip(non_geom_fields, row))
                wkt = row[wkt_field_i].read()
                the_dict[wkt_field] = wkt
                dicts.append(the_dict)
            return dicts


    def _wkt_getter(self, geom_field):
        assert geom_field is not None
        return 'SDO_UTIL.TO_WKTGEOMETRY({}) AS {}_WKT'.format(geom_field, geom_field)


    def read(self, table, fields, geom_field=None, dictify=True, where=None, limit=None):
        '''
        Reads specified fields from a DB table.
        '''
        # Form SQL statement
        fields = list(fields)  # Make a copy
        if fields != ['*']:
            if geom_field:
                fields.append(self._wkt_getter(geom_field)) 
            fields_joined = ', '.join(fields)
            table = table.upper()
            stmt = "SELECT {} FROM {}".format(fields_joined, table)
        else:
            if geom_field:
                print(geom_field)
                stmt = "SELECT {}.*, {} FROM {}".format(table, \
                    self._wkt_getter(geom_field), table)
            else:
                stmt = "SELECT * FROM {}".format(table)
        if where:
            stmt += " WHERE {}".format(where)
            if limit:
                stmt += " AND ROWNUM <= {}".format(limit)
        elif limit:
            stmt += " WHERE ROWNUM <= {}".format(limit)

        try:
            self._c.execute(stmt)
        except cx_Oracle.DatabaseError as e:
            print('Error executing statement:\n{}'.format(stmt))
            raise

        if dictify:
            rows = self._dictify(geom_field=geom_field)
        return rows


    ############################################################################
    # WRITE
    ############################################################################

    def save(self):
        '''
        Commit database changes
        '''
        self.cxn.commit()


    def bulk_insert(self, table, rows, geom_field=None, from_srid=None, \
        multi_geom=True, chunk_size=None):
        '''
        Inserts dictionary row objects in the the database 
        Args: list of row dicts, table name
        '''
        fields = rows[0].keys()
        if geom_field:
            non_geom_fields = [x for x in fields if x != geom_field]
        fields_joined = ', '.join(fields)
        stmt = "INSERT INTO {} ({}) VALUES ".format(table, fields_joined)       

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
                if geom_field:
                    geom = row[geom_field]
                    row[geom_field] = self.prepare_wkt(geom, from_srid, 4326, \
                        multi_geom)

                for field in fields:
                    val = self.prepare_val(row[field])
                    val_row.append(val)
                val_rows.append(val_row)

            # Execute
            vals_joined = ['({})'.format(', '.join(vals)) for vals in val_rows]
            rows_joined = ', '.join(vals_joined)
            cur_stmt += rows_joined
            self._c.execute(cur_stmt)
            self.save()


    def truncate(self, table):
        '''
        Drops all rows from a table
        '''
        stmt = 'DELETE FROM {}'.format(table)
        self._c.execute(stmt)
        self.save()