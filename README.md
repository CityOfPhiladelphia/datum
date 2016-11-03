# Datum
Simple spatial ETL.

## Usage
```python
import datum

db = datum.connect('oracle-stgeom://user:pass@tns_name')
table = db.table('table_name')
rows = table.read()

# read some rows from an oracle db
for row in rows:
    wkt = row['shape']
    print('The geometry for object {} is {}'.format(row['objectid'], wkt))
   
# write out to a local postgres db. the `write` function just needs a list of row dictionaries.
out_db = datum.connect('postgres://user:pass@localhost:5432/db_name')
out_table = out_db.table('table_name')
out_table.write(rows)
```
