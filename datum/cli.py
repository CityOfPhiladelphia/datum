import click
import datum

import logging
log = logging.getLogger(__name__)

@click.group()
def cli():
    pass

@cli.command()
@click.option('--connection', '-d', help='The database connection string', required=True)
@click.argument('table')
def truncate(table, connection):
    db = datum.connect(connection)
    db.table(table).delete()

@cli.command()
@click.option('--connection', '-d', help='The database connection string', required=True)
@click.argument('csvfile', type=click.File('rU'))
@click.argument('table')
def load(csvfile, table, connection):
    db = datum.connect(connection)
    db.table(table).load(csvfile)

@cli.command()
@click.option('--connection', '-d', help='The database connection string', required=True)
@click.argument('sql')
def execute(sql, connection):
    db = datum.connect(connection)
    rows = db.execute(sql)
    if datum.util.isiterable(rows):
        import csv, sys
        writer = csv.writer(sys.stdout)
        writer.writerow(rows.header)
        writer.writerows(rows)

if __name__ == '__main__':
    cli()
