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
import argparse
import pandas as pd
import datetime as dt
import yfinance as yf  
import sys
import time
from psycopg2.errors import UniqueViolation
import sqlalchemy as sql 

from contextlib import redirect_stdout
from tqdm import tqdm

from typing import List

from . import logger
from .database.models import Asset, PriceHistory, session
from .database.functions import (add_new_objects, all_query, 
                                 update_database_object, 
                                 collection_to_dataframe as to_df)

td = dt.date.today()
end_of_day = (dt.datetime.utcnow() + dt.timedelta(hours=3)).date()
date_20y_ago = pd.to_datetime(dt.date(td.year - 20, td.month, td.day))

logger.info(f"End of day: {end_of_day}")
logger.info(f"20 years ago: {date_20y_ago}")

# assets = all_query(Asset)

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

def run_prices(asset: Asset) -> List[PriceHistory]:
    """Create and export new PriceHistory objects."""
    prices = get_prices(asset).reset_index().itertuples(index=False)
    asset.new_prices = [PriceHistory(**p._asdict()) for p in prices]
    # for p in asset.new_prices:
    #     logger.debug(f"{p.date}")
    session.add_all(asset.new_prices)
    session.commit()
    session.refresh(asset)

    return asset.new_prices

if __name__=='__main__':
    from .database.maps import asset_map
    asset = asset_map[1]
    new_prices = run_prices(asset)







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