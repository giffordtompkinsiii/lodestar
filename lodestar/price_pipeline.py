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
import time
import numpy as np
import pandas as pd
import datetime as dt
import yfinance as yf
import multiprocessing as mp

from typing import List

from . import logger
from .database.maps import asset_map
from .database.models import Asset, BuoyHistory, PriceHistory, session
from .database.functions import collection_to_dataframe, on_conflict_do_nothing

class Pipeline(object):
    today = dt.date.today()
    end_of_day = (dt.datetime.utcnow() + dt.timedelta(hours=3)).date()
    date_20y_ago = pd.to_datetime(dt.date(year=today.year - 20, 
                                          month=today.month, 
                                          day=today.day))
    logger.debug(f"End of day: {end_of_day}")
    logger.debug(f"20 years ago: {date_20y_ago}")

    def __init__(self, asset:Asset, debug:bool = False):
        self.asset = asset 
        self.debug = debug 
        
class PricePipeline(Pipeline):
    today = dt.date.today()
    end_of_day = (dt.datetime.utcnow() + dt.timedelta(hours=3)).date()
    date_20y_ago = pd.to_datetime(dt.date(year=today.year - 20, 
                                          month=today.month, 
                                          day=today.day))
    logger.debug(f"End of day: {end_of_day}")
    logger.debug(f"20 years ago: {date_20y_ago}")
    unique_key = 'price_history_asset_id_date_key'

    def get_last_date(self):
        """Get last price record from the database."""
        current_price = self.asset.current_price
        if hasattr(current_price, 'date'):
            last_date = pd.to_datetime(current_price.date) or self.date_20y_ago
        else:
            last_date = self.date_20y_ago
        self.last_date = last_date
        logger.info(f"{self.asset.asset} last date: {self.last_date}")
        return last_date

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.latest_price_date = self.get_last_date()
        self.price_data_start = pd.to_datetime(
                                dt.date(year=self.latest_price_date.year - 21, 
                                        month=self.latest_price_date.month, 
                                        day=self.latest_price_date.day))
        self.new_prices = None

    def get_prices(self, start_date: str = None, 
                   end_date: str = None, debug: bool = None) -> pd.DataFrame:
        """Pull prices from Yahoo!Finance for given `database.models.Asset`.
        
        Returns
        -------
        pandas.DataFrame: (asset_id, date, price)
            DataFrame of asset's price history.
        """
        a = self.asset
        begin_date = pd.to_datetime(start_date) or self.latest_price_date
        end_date = pd.to_datetime(end_date) or self.end_of_day
        history = yf.Ticker(a.asset.replace(' ','-')) \
                    .history(start=begin_date,
                             end=end_date, 
                             debug=(debug or self.debug))
        history = history[history.index > begin_date]
        logger.info(f"{a.asset} price records since last date: "
                    + f"{history.shape[0]}")

        if not asset.price_history_collection and history.empty:
            logger.warning(f"No yfinance records or database records for "
                            + f"{a.asset}")
            return pd.DataFrame()
        
        logger.info("Sleeping for yFinance API.")
        time.sleep(1)

        import_df = history.reset_index()[['Date','Close']] \
                           .rename(columns={'Date':'date','Close':'price'})

        import_df['asset_id'] = a.id
        logger.debug(import_df)
        prices = import_df.itertuples(index=False)
        self.new_prices = [PriceHistory(**p._asdict()) for p in prices]
        return self.new_prices

    def run_prices(self, start_date: str = None, end_date: str = None, 
                         debug: bool = None) -> List[PriceHistory]:
        """Create and export new PriceHistory objects."""
        new_prices = self.get_prices(start_date, end_date, debug)
        on_conflict_do_nothing(new_prices, constraint_name=self.unique_key)
        session.refresh(asset)

    def __repr__(self):
        repr_str = f"{self.__class__.name}(Asset: '{self.asset.asset}', " \
                    + f"last_date: '{str(self.last_date.date())}', " \
                    + f"data_start': '{str(self.price_data_start.date())}', " \
                    + f"new_prices: {len(self.new_prices or [])} records, " \
                    + f"debug: {self.debug})"
        return repr_str

class DailyTideamrk(Pipeline):
    """Pipeline to calculate DailyTidemarks for new PriceHistory records."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    pass

class BuoyPipeline(Pipeline):
    """Pipeline for calcualting buoy history for new prices."""
    per_cols = {'mo_01': 21, 
                'mo_06': 126, 
                'yr_01': 252, 
                'yr_05': 252*5, 
                'yr_10': 252*10, 
                'yr_20': 252*20}

    per_weights = {'mo_01': 4, 
                    'mo_06': 3, 
                    'yr_01': 2, 
                    'yr_05': 1, 
                    'yr_10': 1, 
                    'yr_20': 1}
    unique_key = 'buoy_history_price_id_high_mark_day_mvmt_key'
    numer_func = lambda self, b: b.replace(np.nan, 0.0) * self.per_weights[b.name]
    denom_func = lambda self, b: ~b.isna() * self.per_weights[b.name]

    def __init__(self, new_asset: bool = False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        price_history = sorted(self.asset.price_history_collection, 
                               key=lambda p: p.date, 
                               reverse=True)
        self.latest_price_date = price_history[0].date
        if new_asset:
            p = price_history[-1]
        else:
            # Establish buoy data start date
            for p in price_history:
                logger.debug(f"{p.date}")
                if p.buoy_history_collection:
                    break
        self.buoy_data_start = pd.to_datetime(
                                    dt.date(year=p.date.year - 30, 
                                            month=p.date.month, 
                                            day=p.date.day))
        self.last_buoy_date = p.date
        self.up_to_date = (p.date==self.latest_price_date)

    def __repr__(self):
        repr_str =  f"{self.__class__}(" \
                    + f"buoy_data_start: {self.buoy_data_start}, " \
                    + f"last_buoy_date: {self.last_buoy_date}, " \
                    + f"up_to_date: {self.up_to_date}, " \
                    + f"per_cols: {self.per_cols}, " \
                    + f"per_weights: {self.per_weights})"
        return repr_str

    def _get_price_history(self):
        """Return relevant price history."""
        # Pull history from 20y before last buoy date record.
        self.price_history = list(filter(lambda p: p.date > self.buoy_data_start,
                                         self.asset.price_history_collection))
        prices = self.price_history
        price_df = collection_to_dataframe(prices).reset_index('asset_id')

        # Filter by price records without buoy collections
        self.price_date_index = price_df.sort_index()[self.last_buoy_date:] \
                                        .iloc[1:].id
        self.up_to_date = self.price_date_index.empty
        return price_df.price

    def _build_mvmt_dataframe(self):
        """Build all the relevant columns for buoy dataframe."""
        prices = self._get_price_history()

        mvmt_df = pd.DataFrame({
            interval: prices.pct_change(periods=interval).rename(interval) \
                                for interval in [1, 5, 21, 126, 252]
        })

        return mvmt_df.rename_axis('day_mvmt', axis=1).fillna(method='ffill')

    def _build_water_marks(self):
        """Build historical high-low water mark table."""
        mvmt_df = self._build_mvmt_dataframe()
        high_marks = pd.DataFrame(index=mvmt_df.stack().index)
        low_marks = pd.DataFrame(index=mvmt_df.stack().index)

        per_cols = self.per_cols

        for window, days in per_cols.items():
            high_marks[window] = mvmt_df.rolling(window=days, 
                                                 min_periods=int(days / 2.0)) \
                                        .max() \
                                        .stack() \
                                        .rename(window) \
                                        .astype(float)
            low_marks[window] = mvmt_df.rolling(window=days, 
                                                min_periods=int(days / 2.0)) \
                                        .min() \
                                        .stack() \
                                        .rename(window) \
                                        .astype(float)
        high_marks['high_mark'] = True
        low_marks['high_mark'] = False
        water_marks = pd.concat([high_marks,low_marks]).sort_index()
        self.water_marks = water_marks.loc[self.date_20y_ago:]
        return self.water_marks

    def _get_buoys(self, audit:bool = False):
        """Run buoy history for new prices."""
        # TODO: Is this necessary?
        # if hasattr(self,'buoy_objects'):
        #     return self.buoy_objects
        marks = self._build_water_marks()

        if self.up_to_date:
            logger.info(f"'{self.asset.asset}' BuoyHistory is up-to-date.")
            return []

        buoys = marks.join(self.price_date_index, on='date', how='inner') \
                     .rename(columns={'id':'price_id'}) \
                     .dropna(subset=self.per_cols, how='all')

        # Use class functions to calculate weighted mean water marks
        b = buoys[self.per_cols]
        num = b.apply(self.numer_func).sum(axis=1)
        denom = b.apply(self.denom_func).sum(axis=1)
        buoys['watermark'] = (num / denom).astype(float)

        buoys = buoys.reset_index('day_mvmt')

        self.buoy_objects = [BuoyHistory(**b._asdict()) \
                                for b in buoys.astype(float) \
                                              .itertuples(index=False)]
        return self.buoy_objects

    def run_buoys(self) -> pd.DataFrame():
        """Calculate buoys and add to database."""
        buoy_objects = self._get_buoys()

        if buoy_objects:
            logger.info(f"Adding {len(buoy_objects)} BuoyHistory records for " \
                    + f"'{self.asset.asset}'.")
            on_conflict_do_nothing(buoy_objects, constraint_name=self.unique_key)
            session.refresh(self.asset)
            return collection_to_dataframe(buoy_objects)
        return pd.DataFrame()

class DailyPipeline(Pipeline):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.price_pipeline = PricePipeline(asset=self.asset)
        self.buoy_pipeline = BuoyPipeline(asset=self.asset, new_asset=False)

    def run_daily_procedures(self):
        self.price_pipeline.run_prices(start_date='1990-01-01')
        self.buoy_pipeline.run_buoys()

if __name__=='__main__':
    t_0 = time.time()
    for a, asset in asset_map.items():
        logger.info(f"{asset.id} - {asset} Running Daily Procedures.")
        d = DailyPipeline(asset)
        d.run_daily_procedures()
