import click
import datum

import logging
log = logging.getLogger(__name__)

@click.group()
def cli():
    pass

@cli.command()
@click.option('--connection', '-d', help='The database connection string')
@click.argument('table')
def truncate(table, connection):
    db = datum.connect(connection)
    db.table(table).delete()

if __name__ == '__main__':
    cli()
