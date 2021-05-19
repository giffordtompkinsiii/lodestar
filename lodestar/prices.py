"""Use Yahoo!Finance to import prices since last update

This module holds the functions for pulling prices from the Yahoo!Finance API and 
exporting them to the database.

The __main__ function will run prices for all assets in the database.
Errors are logged in the `data_manager.logger` object.

Resources
---------
yFinance : # https://towardsdatascience.com/best-5-free-stock-market-apis-in-2019-ad91dddec984

Methods
-------
get_prices(a: Asset)->pandas.DataFrame:
    Pull prices from Yahoo!Finance for given asset.
update_prices(asset: Asset, debug: bool=True):
    Update database with records from .get_prices()
"""
import numpy as np
import argparse
import pandas as pd
import datetime as dt
from sqlalchemy.util.langhelpers import methods_equivalent
import yfinance as yf  
import sys
import time
from psycopg2.errors import UniqueViolation
import sqlalchemy as sql 

from contextlib import redirect_stdout
from tqdm import tqdm

from typing import List

from . import logger
from .database.models import Asset, BuoyHistory, PriceHistory, session
from .database.functions import (add_new_objects, all_query, 
                                 update_database_object, collection_to_dataframe)

td = dt.date.today()
end_of_day = (dt.datetime.utcnow() + dt.timedelta(hours=3)).date()
date_20y_ago = pd.to_datetime(dt.date(td.year - 21, td.month, td.day))

logger.info(f"End of day: {end_of_day}")
logger.info(f"20 years ago: {date_20y_ago}")

def get_prices(asset: Asset, last_date: dt.date = None, 
                             debug:bool = False) -> pd.DataFrame:
    """Pull prices from Yahoo!Finance for given `database.models.Asset`.
    
    Returns
    -------
    pandas.DataFrame: (asset_id, date, price)
        DataFrame of asset's price history.
    """
    if hasattr(asset.current_price, 'date'):
        last_date = pd.to_datetime(asset.current_price.date) or date_20y_ago
    else:
        last_date = date_20y_ago

    logger.info(f"{asset.asset} last date: {last_date}")

    history = yf.Ticker(asset.asset.replace(' ','-')) \
                .history(start=last_date,
                         end=end_of_day, 
                         # 9PM is closing time in UTC
                         debug=debug)
    history = history[history.index > last_date]
    logger.info(
        f"{asset.asset} price records since last date: {history.shape[0]}")

    if not asset.price_history_collection and history.empty:
        info.warning(f"No yfinance records or database records for {asset.asset}")
        return pd.DataFrame()
    
    logger.info("Sleeping for yFinance API.")
    time.sleep(1)
    import_df = history.reset_index()[['Date','Close']] \
                       .rename(columns={'Date':'date','Close':'price'})
    import_df['asset_id'] = asset.id

    return import_df.set_index(['asset_id','date'])

def update_buoys(asset: Asset):
    """Update missing buoy values."""
    pass


def get_new_buoys(asset, audit:bool = False):
    """Run buoy history for new prices."""
    try:
        last_run = pd.to_datetime(sorted(asset.new_prices, key=lambda p: p.date)[0].date)
    except:
        last_run = dt.date.today()

    date_20y_ago = pd.to_datetime(dt.date(year=last_run.year - 21, 
                                          month=last_run.month, 
                                          day=last_run.day))

    prices = list(filter(lambda p:p.date>=date_20y_ago,
                             asset.price_history_collection))

    p_index = collection_to_dataframe(prices).reset_index('asset_id')
    price_df = p_index.price

    buoy_df = pd.DataFrame({
        interval: price_df.pct_change(periods=interval, freq=pd.offsets.BDay()) \
                          .rename(interval) for interval in [1, 5, 21, 126, 252]})

    buoy_df = buoy_df.rename_axis('day_mvmt', axis=1).fillna(method='ffill')

    high_marks = pd.DataFrame(index=buoy_df.stack().index)
    low_marks = pd.DataFrame(index=buoy_df.stack().index)

    per_cols = {'mo_01': 21, 
                'mo_06': 126, 
                'yr_01': 252, 
                'yr_05': 252*5, 
                'yr_10': 252*10, 
                'yr_20': 252*20}
    per_weights = [4,3,2,1,1,1]

    for window, days in per_cols.items():
        high_marks[window] = buoy_df.rolling(window=days, min_periods=days) \
                             .max().stack().rename(window)
        low_marks[window] = buoy_df.rolling(window=days, min_periods=days) \
                              .min().stack().rename(window)
    high_marks['high_mark'] = True
    low_marks['high_mark'] = False
    buoys = pd.concat([high_marks,low_marks])
    buoys = buoys.join(p_index.id, on='date') \
                 .rename(columns={'id':'price_id'})
    buoys = buoys.dropna(subset=per_cols, how='all')
    ### TODO: This takes a second, so should be calculted after price_id has been 
    # used to filter new buoys.
    buoys['water_mark'] = buoys[per_cols].apply(
                                lambda b: np.dot(per_weights, 
                                                 b.replace(np.nan,0.0)) \
                                            / np.sum(per_weights * ~b.isna()),
                                axis=1)
    buoys = buoys.reset_index('day_mvmt')
    buoys.price_id = p_index.id

    
    # buoy_objects = [BuoyHistory(**b._asdict()) \
    #                 for b in buoys.itertuples(index=False)]
    # session.add_all(buoy_objects)
    # session.commit()
    # # TODO: Make sure all buoy History columns are present.
    return buoys, p_index


def run_prices(asset: Asset, audit: bool = False) -> List[PriceHistory]:
    """Create and export new PriceHistory objects."""
    prices = get_prices(asset).reset_index().itertuples(index=False)
    asset.new_prices = [PriceHistory(**p._asdict()) for p in prices]
    # for p in asset.new_prices:
    #     logger.debug(f"{p.date}")
    session.add_all(asset.new_prices)
    session.commit()
    session.refresh(asset)

    new_buoys = get_new_buoys(asset, audit)
    logger.info(f"Inserted {len(new_buoys)} records for {asset.asset} Buoy History.")
    return asset.new_prices

if __name__=='__main__':
    from .database.maps import asset_map
    import time
    t_0 = time.time()
    for asset in list(asset_map.values())[0:2]:
        buoys, p_index = get_new_buoys(asset, audit=True)
        print(buoys.head(60))
        print(p_index.head(60))
        print((time.time() - t_0)/60, 'mins')







# def update_records(asset: Asset, debug: bool = False):
#     import_df = get_prices(asset=asset, debug=debug)
#     time.sleep(10)

#     if import_df.empty:
#         return asset

#     import_df = import_df.reset_index()[['Date','Close']] \
#                          .rename(columns={'Date':'date','Close':'price'})
#     import_df['asset_id'] = asset.id

#     return update_database_object(import_df=import_df, 
#                                  db_records=asset.price_history_collection,
#                                  db_table=PriceHistory,
#                                  refresh_object=asset)

# def run_prices(assets=assets, debug=False, asset_list: list = [], reverse=False):
#     asset_mask = None
#     if asset_list:
#         asset_mask = lambda a: a.asset in [a.upper().strip() for a in asset_list]
#         total_assets = len(asset_list) 
#     else:
#         total_assets = len(assets)

#     if reverse:
#         assets = list(reversed(assets))

#     for asset in tqdm(iterable=filter(asset_mask, assets),
#                       total=total_assets, position=0, desc="Collecting prices",
#                       leave=False):
#         update_records(asset=asset, debug=debug)
#     return True


# def main():
#     parser = argparse.ArgumentParser(prog='prices')
#     parser.add_argument('-d', '--debug',
#                         help='run with stdout active',
#                         action='store_true')
#     parser.add_argument('-l', '--list',
#                          help='input list of specific assets to pull, separated by spaces.',
#                          dest='asset_list',
#                          action='extend',
#                          nargs="+", 
#                          default=None,
#                          type=str)
#     args = parser.parse_args()

#     if args.debug:
#         print(f"{'=' * len(vers_message)}\n{vers_message}\n{'=' * len(vers_message)}")
#         print(args.__dict__)

#     run_prices(debug=args.debug, asset_list=args.asset_list)

#     session.commit()