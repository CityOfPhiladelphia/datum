# Datum
Simple spatial ETL.

## Usage
```python
import datum

db = datum.connect('oracle-stgeom://user:pass@tns_name')
table = db.table('table_name')
rows = table.read()

for row in rows:
    wkt = row['shape']
```
