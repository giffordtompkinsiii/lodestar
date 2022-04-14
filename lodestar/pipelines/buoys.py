from . import *
from dateutil.relativedelta import relativedelta

class BuoyPipeline(AssetPipeline):
    """AssetPipeline for calculating buoy history for new prices.
    
    Attributes
    ==========
    
    Methods
    =======
    """
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

    def __init__(self, asset: Asset, debug: bool = False):
        super().__init__(asset, debug)
        # Pull the price history of the asset and sort in reverse-date order.
        price_history = sorted(self.asset.price_history_collection, 
                               key=lambda p: p.date, 
                               reverse=True)
        
        # Create default latest date. 
        # TODO: Is this best? Should it be a moving date?
        self.latest_price_date = (dt.date.today() - relativedelta(years=20))

        # Try to set the latest price date.
        try:
            self.latest_price_date = price_history[0].date
        except:
            logger.warning(f"{asset.asset} has no price history. Import price history to get price movement.")
            return

        # Loop through price history in reverse order looking for latest record with a buoy history.
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

    def get_price_history(self):
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

    def build_mvmt_dataframe(self):
        """Build all the relevant columns for buoy dataframe."""
        prices = self.get_price_history()

        mvmt_df = pd.DataFrame({
            interval: prices.pct_change(periods=interval).rename(interval) \
                                for interval in [1, 5, 21, 126, 252]
        })

        return mvmt_df.rename_axis('day_mvmt', axis=1).fillna(method='ffill')

    def build_water_marks(self):
        """Build historical high-low water mark table."""
        mvmt_df = self.build_mvmt_dataframe()
        high_marks = pd.DataFrame(index=mvmt_df.stack().index)
        low_marks = pd.DataFrame(index=mvmt_df.stack().index)

        per_cols = self.per_cols

        for window, days in per_cols.items():
            high_marks[window] = mvmt_df.rolling(window=days, min_periods=((
                                                (days < 252) * days) or 252)) \
                                    .max() \
                                    .stack() \
                                    .rename(window) \
                                    .astype(float)
            low_marks[window] = mvmt_df.rolling(window=days, min_periods=((
                                                (days < 252) * days) or 252)) \
                                    .min() \
                                    .stack() \
                                    .rename(window) \
                                    .astype(float)
        high_marks['high_mark'] = True
        low_marks['high_mark'] = False
        water_marks = pd.concat([high_marks,low_marks]).sort_index()
        self.water_marks = water_marks.loc[self.date_21y_ago:]
        return self.water_marks

    def get_buoys(self, audit:bool = False):
        """Run buoy history for new prices."""
        # TODO: Is this necessary?
        # if hasattr(self,'buoy_objects'):
        #     return self.buoy_objects
        marks = self.build_water_marks()

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
        buoy_objects = self.get_buoys()

        if buoy_objects:
            logger.info(f"Adding {len(buoy_objects)} BuoyHistory records for " \
                    + f"'{self.asset.asset}'.")
            on_conflict_do_nothing(buoy_objects, constraint_name=self.unique_key)
            session.refresh(self.asset)
            return collection_to_dataframe(buoy_objects)
        return pd.DataFrame()

if __name__=='__main__':
    for asset in asset_map.values():
        b = BuoyPipeline(asset=asset, debug=False)
        if b.asset.price_history_collection:
            b.run_buoys()