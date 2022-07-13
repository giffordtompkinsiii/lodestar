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

import os
import numpy as np
import psycopg2.extensions as psyco

from sqlalchemy import create_engine

from lodestar import logger


def nan_to_null(f, _NULL=psyco.AsIs('NULL'), _Float=psyco.Float):
    if not np.isnan(f):
        return _Float(f)
    return _NULL


def nat_to_null(f, _NULL=psyco.AsIs('NULL'), _Date=psyco.DATE):
    if not np.isnat(f):
        return _Date(f)
    return _NULL


psyco.register_adapter(float, nan_to_null)
psyco.register_adapter(np.datetime64, nat_to_null)
home_dir = os.environ['HOME']
psql_root_dir = os.path.join(home_dir, '.postgresql', 'lodestar')

connect_args = {
    'sslmode': 'verify-ca',
    'sslcert': os.path.join(psql_root_dir, 'postgres.crt'),
    'sslkey': os.path.join(psql_root_dir, 'postgres.key'),
    'sslrootcert': os.path.join(psql_root_dir, 'root.crt'),
    'connect_timeout': 5400
}


def formatting_proxy(home_directory):
    logger.info("Formatting proxy")
    proxy_str = os.path.join(home_directory, "cloud_sql_proxy")
    proxy_dir = os.path.join(home_directory, "cloudsql")
    instances = "lodestar:us-central1:tidesgroup"
    username = os.environ['TTG_USERNAME'] or input("Username: ")
    password = os.environ['TTG_PASSWORD'] or input("Password: ")

    from sqlalchemy.engine.url import URL
    engine_url = URL(
        drivername="postgresql+psycopg2",
        username=username,
        password=password,
        host=os.path.join(proxy_dir, instances),
        port=5432,
        database="datahull",
    )
    return engine_url


url = formatting_proxy(home_dir)
engine = create_engine(url, connect_args=connect_args)

if __name__ == '__main__':
    logger.info(url)
