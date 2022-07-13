"""SQLAlchemy Database Objects

This module holds the database objects from teh SQLAlchemy Library and 
their accompanying object modifications. 

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

from sqlalchemy import exc as sa_exc, orm, schema, ext
from sqlalchemy import (Boolean, Column, Date, DateTime, ForeignKey,
                        Integer, Text, UniqueConstraint)

import warnings

metadata = schema.MetaData(bind=engine,
                           schema='financial')

tables = ['accounts',
          'account_types',
          'apis',
          'assets',
          'balance_history',
          'buoy_history',
          'clients',
          'position_history',
          'price_history',
          'tidemarks',
          'tidemark_history',
          'tidemark_history_daily',
          'tidemark_terms',
          'tidemark_types',
          'transaction_history',
          'typed_tidemarks',
          'vw_algotrading',
          'vw_believability',
          'vw_bloomberg',
          'vw_high_watermarks',
          'vw_low_watermarks']

# Extract MetaData and create tables from them using AutoMap.
with warnings.catch_warnings():
    warnings.simplefilter('ignore', sa_exc.SAWarning)

    metadata.reflect(
        extend_existing=True,
        views=True,
        only=tables)

Base = ext.automap.automap_base(metadata=metadata)


# >>> INSERT VIEW DEFINITIONS HERE
class AlgoTrading(Base):
    __tablename__ = 'vw_algotrading'
    __table_args__ = {'extend_existing': True}
    asset_id = Column(Integer, ForeignKey('assets.id'), primary_key=True)
    unique_constraint = UniqueConstraint(asset_id)


class Bloomberg(Base):
    __tablename__ = 'vw_bloomberg'
    __table_args__ = {'extend_existing': True}
    asset_id = Column(Integer, ForeignKey('assets.id'), primary_key=True)
    tidemark_id = Column(Integer, ForeignKey('tidemarks.id'), primary_key=True)
    unique_constraint = UniqueConstraint(asset_id, tidemark_id)


class CurrentBelievability(Base):
    __tablename__ = 'vw_believability'
    __table_args__ = {'extend_existing': True}
    asset_id = Column(Integer, ForeignKey('assets.id'), primary_key=True)
    date = Column(Date, primary_key=True)
    unique_constraint = UniqueConstraint(asset_id, date)


class Watermark(Base):
    __tablename__ = 'vw_watermarks'
    __table_args__ = {'extend_existing': True}
    asset_id = Column(Integer, ForeignKey('assets.id'), primary_key=True)
    date = Column(Date, primary_key=True)
    high_mark = Column(Boolean, primary_key=True)
    day_mvmt = Column(Integer, primary_key=True)
    unique_constraint = UniqueConstraint(asset_id,
                                         date,
                                         high_mark,
                                         day_mvmt)


class LowWatermark(Watermark):
    __tablename__ = 'vw_low_watermarks'
    __table_args__ = {'extend_existing': True}
    asset_id = Column(Integer,
                      ForeignKey('vw_watermarks.asset_id'),
                      primary_key=True)
    date = Column(Date, primary_key=True)
    high_mark = Column(Boolean, primary_key=True)
    day_mvmt = Column(Integer, primary_key=True)
    unique_constraint = UniqueConstraint(asset_id,
                                         date,
                                         day_mvmt)


class HighWatermark(Watermark):
    __tablename__ = 'vw_high_watermarks'
    __table_args__ = {'extend_existing': True}
    asset_id = Column(Integer,
                      ForeignKey('vw_watermarks.asset_id'),
                      primary_key=True)
    date = Column(Date, primary_key=True)
    high_mark = Column(Boolean, primary_key=True)
    day_mvmt = Column(Integer, primary_key=True)
    unique_constraint = UniqueConstraint(asset_id,
                                         date,
                                         day_mvmt)


def name_for_collection_relationship(base, local_cls, referred_cls, constraint):
    reflexive_names = {
        'tidemark_terms_term_id_fkey': 'tidemarks_collection',
        'tidemark_terms_tidemark_id_fkey': 'terms_collection',
        'growth_tidemark_terms_term_id_fkey': 'growth_tidemarks_collection',
        'growth_tidemark_terms_tidemark_id_fkey': 'growth_terms_collection'
    }

    if local_cls == referred_cls:
        try:
            return reflexive_names[constraint.name]
        except KeyError as ke:
            logger.critical("Missing ReflexiveKey for:", local_cls.__name__)
            logger.critical("Constraint Name:", constraint.name)
    else:
        return referred_cls.__name__.lower() + "_collection"


with warnings.catch_warnings():
    warnings.simplefilter('ignore', sa_exc.SAWarning)
    Base.prepare(reflect=True,
                 name_for_collection_relationship=name_for_collection_relationship)

# TABLE DEFINITIONS
Account = Base.classes.accounts
AccountType = Base.classes.account_types
Api = Base.classes.apis
Asset = Base.classes.assets
BalanceHistory = Base.classes.balance_history
BuoyHistory = Base.classes.buoy_history
Client = Base.classes.clients
PositionHistory = Base.classes.position_history
PriceHistory = Base.classes.price_history
Tidemark = Base.classes.tidemarks
TidemarkDaily = Base.classes.tidemark_history_daily
TidemarkHistory = Base.classes.tidemark_history
TidemarkType = Base.classes.tidemark_types
TransactionHistory = Base.classes.transaction_history

# CUSTOM RELATIONSHIP DEFINITIONS
Asset.current_price = orm.relationship(PriceHistory,
                                       primaryjoin=(Asset.id == PriceHistory.asset_id),
                                       order_by=lambda: PriceHistory.date.desc(),
                                       uselist=False,
                                       overlaps='price_history_collection')

Account.current_balance = orm.relationship(
    BalanceHistory,
    primaryjoin=(
            Account.id == BalanceHistory.account_id),
    order_by=lambda: BalanceHistory.date.desc(),
    uselist=False,
    overlaps='balance_history_collection')

# Instantiate a session for querying.
Session = orm.sessionmaker()
session = Session(bind=engine, expire_on_commit=False)

if __name__ == '__main__':
    for c in Base.classes:
        print(c.__table__.name)
