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

## Installation

### Setting up Oracle on OS X
Following [this guide](https://web.archive.org/web/20160407232743/http://kevindalias.com/2014/03/26/how-to-set-up-cx_oracle-for-python-on-mac-os-x-10-89):

1. Install the Oracle Instant Client. Download the 64-bit versions of the basic and sdk zip files [from oracle](http://www.oracle.com/technetwork/topics/intel-macsoft-096467.html).
2. Create a global oracle directory in a location such as `~/.local/share/oracle` and copy the two `.zip` files into it
3. Unzip the `.zip` files into that directory. When finished, the `oracle` directory should contain a bunch of files in it (rather than containing a single directory of files).
4. Inside the `oracle` directory, create symbolic links using:

```bash
ln -s libclntsh.dylib.11.1 libclntsh.dylib
ln -s libocci.dylib.11.1 libocci.dylib
```

Finally, add the following environment variables to your `~/.bash_profile`, replacing the value of `ORACLE_HOME` with the absolute path to your new `oracle` directory.

```bash
export ORACLE_HOME="/path/to/oracle"
export DYLD_LIBRARY_PATH=$ORACLE_HOME
export LD_LIBRARY_PATH=$ORACLE_HOME
```
