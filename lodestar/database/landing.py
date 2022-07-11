"""SQLAlchemy Database Objects

This module holds the database objects from the Landing Schema Objects.

Notes + References
==================
Adding and Updating Objects : 
        https://docs.sqlalchemy.org/en/13/orm/tutorial.html#adding-and-updating-objects

Materialized View Methods from Jeff Widman: 
        http://www.jeffwidman.com/blog/847/using-sqlalchemy-to-create-and-manage-postgresql-materialized-views/


Objects
=======
tables : list
    table names from the database from which to make objects
"""
from lodestar.database import engine, logger
from sqlalchemy import Column, Date, Text
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy import MetaData

landing_metadata = MetaData(bind=engine,
                            schema='landing')

Base = automap_base(metadata=landing_metadata)


class LandingTable(object):

    @classmethod
    def get_conn(cls):
        meta = cls.metadata
        return meta.bind.engine

    @classmethod
    def truncate_table(cls):
        conn = cls.get_conn()
        conn.execute(f"TRUNCATE {meta.schema}.{cls.__tablename__}")

    def all_query(self):
        conn = self.__class__.get_conn()
        query = f"SELECT * FROM {self.__class__.metadata.schema}.{self.__tablename__}"
        return conn.execute(query)

    # @classmethod
    # def drop_table(cls):
    #     meta = cls.metadata
    #     conn = meta.bind.engine
    #     conn.execute(f"DROP TABLE {meta.schema}.{cls.__tablename__}")


class Asset(Base, LandingTable):
    __tablename__ = 'assets'
    symbol = Column(Text, primary_key=True)


class TidemarkHistory(Base, LandingTable):
    __tablename__ = 'tidemark_history'
    date = Column(Date, primary_key=True)
    security = Column(Text, primary_key=True)


class PriceHistory(Base, LandingTable):
    __tablename__ = 'price_history'
    date = date = Column(Date, primary_key=True)
    symbol = Column(Text, primary_key=True)


Base.prepare(reflect=True)

if __name__ == '__main__':
    pass
