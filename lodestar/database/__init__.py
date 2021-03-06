"""Module handles all connections to the postgresql database.

Included in this module are the `functions` and `models` submodules.
Connection is based on presence of 'wokring ip address` (`WIP`) environment 
variable. If it is missing, we will invoke the proxy.

Submodules
----------
models
    SQLAlchemy Database Objects
functions
    Database-to-Pandas-DataFrame compatability functions.
"""
import warnings
from sqlalchemy import exc as sa_exc
import os
import subprocess
from urllib import parse
from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData

home_dir = os.environ['HOME']
psql_root_dir = os.path.join(home_dir,'.postgresql','lodestar')

connect_args = {
    'sslmode': 'verify-ca',
    'sslcert': os.path.join(psql_root_dir, 'postgres.crt'),
    'sslkey': os.path.join(psql_root_dir, 'postgres.key'),
    'sslrootcert': os.path.join(psql_root_dir, 'root.crt'),
    'connect_timeout': 5400
}

def formatting_proxy(home_directory):
    """Format url for connection proxy."""
    # print("Formatting proxy")
    proxy_str = os.path.join(home_directory,"cloud_sql_proxy") 
    proxy_dir = os.path.join(home_directory,"cloudsql")
    instances = "lodestar:us-central1:tidesgroup"
    username = os.environ['TTG_USERNAME'] or input("Username: ")
    password = os.environ['TTG_PASSWORD'] or input("Password: ")

    from sqlalchemy.engine.url import URL
    url = URL(
        drivername="postgresql+psycopg2",
        username=username,
        password=password,
        host=os.path.join(proxy_dir, instances),
        port=5432,
        database="datahull",
    )
    return url

url = formatting_proxy(home_dir)
engine = create_engine(url, connect_args=connect_args)
metadata = MetaData(bind=engine, schema='financial')

if __name__=='__main__':
    print(url)
