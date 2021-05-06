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
import warnings

from . import engine, metadata, logger

from sqlalchemy import (Boolean, Column, Date, DateTime, ForeignKey, Float, 
                        Integer, Table, Text, UniqueConstraint, exc as sa_exc)
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.automap import automap_base


tables = ['accounts',
            'account_types',
            'api',
            'assets',
            'cash_balance_history',
            'clients',
            'price_history',
            'tidemarks',
            'tidemark_history',
            'tidemark_history_daily',
            'tidemark_terms',
            'tidemark_types',
            'trade_history']

# Extract MetaData and create tables from them using AutoMap.
with warnings.catch_warnings():
    warnings.simplefilter('ignore', sa_exc.SAWarning)

    metadata.reflect(
        extend_existing=True,
        views=True,
        only=tables)

Base = automap_base(metadata=metadata)

## >>> INSERT VIEW DEFINITIONS HERE
def name_for_collection_relationship(base, local_cls, referred_cls, constraint):
    reflexive_names = {
        'tidemark_terms_term_id_fkey': 'tidemarks_collection',
        'tidemark_terms_tidemark_id_fkey' : 'terms_collection',
        'growth_tidemark_terms_term_id_fkey' : 'growth_tidemarks_collection',
        'growth_tidemark_terms_tidemark_id_fkey' : 'growth_terms_collection'
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

## TABLE DEFINITIONS
Account = Base.classes.accounts
AccountType = Base.classes.account_types
Api = Base.classes.apis
Asset = Base.classes.assets
CashBalance = Base.classes.cash_balance_history
Client = Base.classes.clients
PriceHistory = Base.classes.price_history
TidemarkHistory = Base.classes.tidemark_history
TidemarkDaily = Base.classes.tidemark_history_daily
Tidemark = Base.classes.tidemarks
TidemarkType = Base.classes.tidemark_types
TradeHistory = Base.classes.trade_history


## CUSTOM RELATIONSHIP DEFINITIONS
with warnings.catch_warnings():
    warnings.simplefilter('ignore', sa_exc.SAWarning)
    Asset.current_price = relationship(PriceHistory,
                                primaryjoin=(Asset.id==PriceHistory.asset_id),
                                order_by=lambda: PriceHistory.date.desc(),
                                uselist=False)

# Instantiate a session for querying.
Session = sessionmaker()
session = Session(bind=engine, expire_on_commit=False)

if __name__=='__main__':
    for c in Base.classes:
        print(c.__table__.name)


## VIEW DEFINITIONS
# class AlgoTrading(Base):
#     __tablename__='vw_algotrading'
#     __table_args__={'extend_existing': True}
#     asset_id = Column(Integer, ForeignKey('assets.id'), primary_key=True)
#     asset = Column(Text, ForeignKey('assets.asset'), primary_key=True)
#     date = Column(Date, primary_key=True)
#     in_portfolio = Column(Boolean)
#     trans = Column(Integer)
#     bound = Column(Float)
#     closing_price = Column(Float)
#     unique_constraint = UniqueConstraint(asset, date) 

# class BelievabilityReport(Base):
#     __tablename__='vw_believability'
#     __table_args__={'extend_existing': True}
#     asset = Column(Text, ForeignKey('assets.asset'), primary_key=True)
#     date = Column(Date, primary_key=True)
#     unique_constraint = UniqueConstraint(asset, date) 

# class Bloomberg(Base):
#     __tablename__='vw_bloomberg'
#     __table_args__={'extend_existing': True}
#     asset_id = Column(Integer, ForeignKey('assets.id'), primary_key=True)
#     tidemark_id = Column(Integer, ForeignKey('tidemarks.id'), primary_key=True)
#     unique_constraint = UniqueConstraint(asset_id, tidemark_id)

# class BuoyUpdates(Base):
#     __tablename__='vw_buoy_updates'
#     __table_args__={'extend_existing': True}
#     asset_id = Column(Integer, ForeignKey('assets.id'), primary_key=True)
#     asset = Column(Text, ForeignKey('assets.asset'), primary_key=True)
#     price_id = Column(Integer, ForeignKey('price_history.id'), primary_key=True)
#     date = Column(Date, primary_key=True)
#     unique_constraint = UniqueConstraint(price_id)

# class CurrentBelievability(Base):
#     __tablename__='vw_current_believability'
#     __table_args__ = {'extend_existing': True}
#     asset_id = Column(Integer, ForeignKey('assets.id'), primary_key=True)
#     date = Column(Date, primary_key=True)
#     believability = Column(Float)
#     confidence = Column(Float)
#     unique_constraint = UniqueConstraint(asset_id, date)

# class CurrentBuoy(Base):
#     __tablename__='vw_current_buoy'
#     __table_args__ = {'extend_existing': True}
#     asset_id = Column(Integer, ForeignKey('assets.id'), primary_key=True)
#     date = Column(Date, primary_key=True)
#     pop = Column(Boolean, primary_key=True)
#     mvmt_int_days = Column(Integer, primary_key=True)
#     price_id = Column(ForeignKey('price_history.id'), primary_key=True)
#     price = Column(Float)
#     mo_01 = Column(Float)
#     mo_06 = Column(Float)
#     yr_01 = Column(Float)
#     yr_05 = Column(Float)
#     yr_10 = Column(Float)
#     yr_20 = Column(Float)
#     wtd_avg = Column(Float)
#     unique_constraint = UniqueConstraint(asset_id, date, pop, mvmt_int_days)

# class CurrentPosition(Base):
#     __tablename__='vw_current_positions'
#     __table_args__={'extend_existing': True}
#     client = Column(Text, ForeignKey('clients.client'), primary_key=True)
#     account_id = Column(Integer, ForeignKey('accounts.id'), primary_key=True)
#     asset = Column(Text, ForeignKey('assets.asset'), primary_key=True)
#     unique_constraint = UniqueConstraint(client, account_id, asset)

# class Drop(Base):
#     __tablename__='vw_drop'
#     __table_args__={'extend_existing': True}
#     asset_id = Column(Integer, ForeignKey('assets.id'), primary_key=True)
#     date = Column(Date, primary_key=True)
#     pop = Column(Boolean, primary_key=True)
#     mvmt_int_days = Column(Integer, primary_key=True)
#     unique_constraint = UniqueConstraint(asset_id, date, pop, mvmt_int_days)

# class PriceMovement(Base):
#     __tablename__='vw_price_movement'
#     __table_args__ = {'extend_existing': True}
#     asset_id = Column(Integer, ForeignKey('assets.id'), primary_key=True)
#     date = Column(Date, primary_key=True)
#     unique_constraint = UniqueConstraint(asset_id, date)

# class PopDrop(Base):
#     __tablename__='vw_pop_drop'
#     __table_args__={'extend_existing': True}
#     price_id = Column(Integer, ForeignKey('price_history.id'), primary_key=True)
#     asset_id = Column(Integer, ForeignKey('assets.id'), primary_key=True)
#     date = Column(Date, primary_key=True)
#     pop = Column(Boolean, primary_key=True)
#     mvmt_int_days = Column(Integer, primary_key=True)
#     unique_constraint = UniqueConstraint(asset_id, date, pop, mvmt_int_days)

# class Pop(Base):
#     __tablename__='vw_pop'
#     __table_args__={'extend_existing': True}
#     asset_id = Column(Integer, ForeignKey('assets.id'), primary_key=True)
#     date = Column(Date, primary_key=True)
#     pop = Column(Boolean, primary_key=True)
#     mvmt_int_days = Column(Integer, primary_key=True)
#     unique_constraint = UniqueConstraint(asset_id, date, pop, mvmt_int_days)    

# class TradeLog(Base):
#     __tablename__='vw_trade_log'
#     __table_args__={'extend_existing': True}
#     asset = Column(Text, ForeignKey('assets.asset'), primary_key=True)
#     account_id = Column(Text, ForeignKey('accounts.id'), primary_key=True)
#     api = Column(Text, ForeignKey('apis.api'), primary_key=True)
#     timestamp = Column(DateTime, primary_key=True)
#     unique_constraint = UniqueConstraint(account_id, api, asset, timestamp) 


## TABLE IMPORTS
# Account = Base.classes.accounts
# AccountType = Base.classes.account_types
# Api = Base.classes.apis
# Asset = Base.classes.assets
# BelievabilityHistory = Base.classes.believability_history
# BuoyHistory = Base.classes.buoy_history
# Client = Base.classes.clients
# IbkrTradeHistory = Base.classes.ibkr_trade_history
# # PopDrop = Base.classes.pop_drop
# Portfolio = Base.classes.portfolios
# Position = Base.classes.positions
# PriceHistory = Base.classes.price_history
# Tidemark = Base.classes.tidemarks
# TidemarkDaily = Base.classes.tidemark_history_daily
# TidemarkHistory = Base.classes.tidemark_history_qtrly
# TidemarkVariable = Base.classes.tidemark_variables
# TradeHistory = Base.classes.trade_history
# TradeType = Base.classes.trade_types
# Variable = Base.classes.variables