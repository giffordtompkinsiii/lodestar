"""Daily and Quarterly Tidemark Calculations

This module contains all necessary functions to generate the daily and quarterly ratios.

Submodules
----------
daily : module
    Script that returns the daily tidemarks for a given asset.
quarterly : module
    Script that returns the daily tidemarks for a given asset.
ratios : module
    Daily and Quarterly Ratio Dictionaries.
"""


import os
import pickle 
os.environ['WIP'] = ''

import numpy as np
import pandas as pd
import datetime as dt

from tqdm import tqdm
from typing import List

from .. import logger, data_file_dir
from ..price_pipeline import Pipeline
from ..database.maps import asset_map, tidemark_map
from ..database.models import (Asset, PriceHistory, TidemarkDaily, TidemarkType, 
                               session)
from ..database.functions import all_query, collection_to_dataframe
# TODO: This import makes me nervous.
from ..pipelines.believability import get_believability



class TidemarkPipeline(Pipeline):
    debug = False
    logger.info(f"Debug set to {debug}.")
    tt_map = {tt.id: tt for tt in all_query(TidemarkType)}
    scores_pickle_path = os.path.join(data_file_dir, 'scores_all.pickle')
    pickle_path = os.path.join(data_file_dir, 'believability.pickle')


    tm_id_map = {i:tm.tidemark for i, tm in tidemark_map.items()}
    id_tm_map = {tm:i for i, tm in tm_id_map.items()}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        pass


    def format_tidemarks(asset: Asset, tidemarks_collection: list)->pd.DataFrame:
        """Format the tidemarks.

        Parameter
        ---------
        tidemarks_day: list 
            list of elements from an `asset`.`tidemark_history_qtrly` collection.

        Returns
        -------
        tidemarks_df: pd.DataFrame
            formatted dataframe for use in the `get_scores` function.
        """

        if not tidemarks_collection:
            logger.warn(f"No tidemarks for [{asset.id} - {asset.asset}]. Returning empty dataframe")
            return pd.DataFrame()

        tidemarks_df = collection_to_dataframe(tidemarks_collection)[['value']].unstack('tidemark_id')\
                                                    .droplevel(axis=1, level=0)
        tidemarks_df = tidemarks_df.reorder_levels(['asset_id','date'])
        for d in pd.date_range(start=tidemarks_df.index.levels[1].max(), 
                            end=dt.date.today() + pd.offsets.QuarterEnd(n=0), 
                            freq='Q').values[1:]:
            tidemarks_df = tidemarks_df.reindex(tidemarks_df.index.insert(loc=-1, item=(asset.id, d)))
        tidemarks_df = tidemarks_df.sort_index().fillna(method='ffill')
        logger.debug(tidemarks_df.index.names)
        logger.debug(tidemarks_df.columns)
        return tidemarks_df.rename(columns=lambda col: tidemark_map[col].tidemark, 
                                level='tidemark_id')

    def format_growth_tidemarks(asset: Asset, 
                                tidemarks_collection: list, 
                                debug: bool = False)->pd.DataFrame:
        """Format the tidemarks.

        Parameter
        ---------
        tidemarks_day: list 
            list of elements from an `asset`.`tidemark_history_qtrly` collection.

        Returns
        -------
        tidemarks_df: pd.DataFrame
            formatted dataframe for use in the `get_scores` function.
        """

        if not tidemarks_collection:
            logger.warn(f"No tidemarks for [{asset.id} - {asset.asset}]. Returning empty dataframe")
            return pd.DataFrame()

        df = collection_to_dataframe(tidemarks_collection)[['id','value']]
        df['id_value'] = list(zip(df.id, df.value))
        df = df[['id_value']].unstack('tidemark_id') \
                            .rename(columns=lambda col: tidemark_map[col].tidemark, 
                                    level='tidemark_id')
        df = df.reorder_levels(['asset_id','date'])

        return df

    def get_scores(dataframe: pd.DataFrame, daily: False):
        '''Return the rolling median, standard deviation and scores.

        Parameters
        ==========
        dataframe: pd.DataFrame()
            Tidemark values with `date` type index.
        daily: bool
            Whether the scores are to be calculated on a daily or quarterly basis.

        Returns
        =======
        meds: pd.DataFrame
            20-year rolling medians.
        stds: pd.DataFrame
            20-year rolling standard deviations.
        scores: pd.DataFrame
            Daily tidemark scores.
        '''
        freq_per_yr = (daily * 252) or 4

        if dataframe.empty:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        meds = dataframe.rolling(window=freq_per_yr * 20, 
                                min_periods=freq_per_yr)\
                        .median() \
                        .reset_index(['asset_id','date'], drop=True)

        stds = dataframe.rolling(window=freq_per_yr * 20, 
                                min_periods=freq_per_yr)\
                        .std() \
                        .reset_index(['asset_id','date'], drop=True)

        scores = (0.5 + (dataframe - meds) / (2 * 1.382 * stds)) \
                        .reset_index(['asset_id','date'], drop=True)
                        
        return meds, stds, scores


class DailyTidemarkPipeline(TidemarkPipeline):
    daily_cols = {
        t.id: t for t in [
            getattr(tm, 'terms_collection', []) \
                for tm in tidemark_map.values() if tm.daily
        ] for term in t
    }

    def __init__(self, price: PriceHistory, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.price = price

    def create_daily_tidemarks(self) -> pd.DataFrame:
        """Generate the daily tidemark columns.

        Parameters
        ----------
        price : PriceHistory
            PriceHistory record passed from prices module.
        """
        a = self.price.assets
        logger.info(f"Calculating {a.asset} TidemarkDaily - {self.price.date}")
        tidemarks = collection_to_dataframe(self.price.tidemark_history_collection)
        v = tidemarks.reset_index(['date','asset_id']) \
                        .value \
                        .rename(index=self.tm_id_map)
        v['price'] = float(self.price.price)

        for col_name in ['ard_preferred_stock', 
                            'bs_sh_out',
                            'cash_and_st_investments',
                            'cf_cap_expend_inc_fix_asset',
                            'cf_cash_from_oper',
                            'eps_growth',
                            'is_operating_expn',
                            'net_chng_lt_debt',
                            'sales_rev_turn',
                            'short_and_long_term_debt',
                            'tot_common_eqy',
                            'trail_12m_cost_of_matl',
                            'trail_12m_minority_int',
                            'trail_12m_net_inc_avai_com_share']:
            if col_name not in v.index:
                v[col_name] = np.nan
                logger.warning(f"{a.asset} missing {col_name}.")

        daily_tm = pd.Series(dtype='float', name='value')

        daily_tm['best_peg_ratio'] = (v.price * v.bs_sh_out
                                        ) / ((v.trail_12m_net_inc_avai_com_share \
                                            * v.eps_growth) or np.nan)
        daily_tm['pe_ratio'] = (v.price * v.bs_sh_out
                                ) / (v.trail_12m_net_inc_avai_com_share or np.nan)
        daily_tm['px_to_book_ratio'] = (v.price * v.bs_sh_out
                                        ) / (v.tot_common_eqy or np.nan)
        daily_tm['current_ev_to_t12m_ebitda'] = (
                            v.price * v.bs_sh_out \
                                + v.ard_preferred_stock \
                                + v.short_and_long_term_debt \
                                + v.trail_12m_minority_int \
                                - v.cash_and_st_investments
                                ) / ((v.sales_rev_turn \
                                        - v.trail_12m_cost_of_matl \
                                        - v.is_operating_expn) or np.nan)
        daily_tm['px_to_free_cash_flow'] = (v.price
                                                ) / ((v.cf_cash_from_oper \
                                                    - v.cf_cap_expend_inc_fix_asset \
                                                    - v.net_chng_lt_debt
                                                    ) or np.nan)
        daily_tm['current_ev_to_t12m_fcf'] = v.price * (v.bs_sh_out \
                                                + v.ard_preferred_stock \
                                                + v.short_and_long_term_debt \
                                                + v.trail_12m_minority_int \
                                                - v.cash_and_st_investments
                                            ) / ((v.cf_cash_from_oper \
                                                    - v.cf_cap_expend_inc_fix_asset \
                                                    - v.net_chng_lt_debt) or np.nan)
        daily_tm['px_to_sales_ratio'] = v.price / (v.sales_rev_turn or np.nan)
        daily_tm = daily_tm.rename(self.id_tm_map) \
                           .rename_axis('tidemark_id') \
                           .reset_index()

        return daily_tm 

    def get_daily_tidemark_objects(self) -> List[TidemarkDaily]:
        daily_tm = self.create_daily_tidemarks()
        daily_tm_objs = [TidemarkDaily(price_id=self.price.id, **d._asdict()) \
                            for d in daily_tm.itertuples(index=False) \
                                if not np.isnan(d.value)]
        self.price.tidemark_history_daily_collection = daily_tm_objs
        return self.price 

    def get_daily_scores(self):
        a = self.price.assets
        tm_history = self.get_tm_history()

        price = self.get_daily_tidemark_objects()

        if not self.price.tidemark_history_daily_collection:
            return []
        prices = collection_to_dataframe([price])
        tm_df = collection_to_dataframe(
                    [*tm_history, *self.price.tidemark_history_daily_collection])
        full_df = prices.join(tm_df.unstack('tidemark_id').value, on='id') \
                        .set_index('id', append=True)

        meds, stds, scores = self.get_scores(full_df, daily=True)

        for d in self.price.tidemark_history_daily_collection:
            d.med_20y = meds.loc[d.price_id, d.tidemark_id]
            d.std_20y = stds.loc[d.price_id, d.tidemark_id]
            d.score = scores.loc[d.price_id, d.tidemark_id]

        session.add_all(self.price.tidemark_history_daily_collection)
        session.commit()
        session.refresh(price)
        price = get_believability(price)
        session.refresh(self.price.assets)

    def get_tm_history(self):
        a = self.price.assets
        tm_history = session.query(TidemarkDaily) \
                            .filter(TidemarkDaily.price_id.in_(
                                [p.id for p in a.price_history_collection])
                            ).all()
        return tm_history


    def run_daily_tidemark(self):
        """Insert new daily tidemarks and return records.

        Refreshes `price` record after being passed.

        Arguements
        ==========
        PriceHistory
            Record from price_history used to calculate new TidemarkDaily records.
        
        Returns
        =======
        list(TidemarkDaily)
            Records for the tidemark_history_daily table for the given PriceHistory 
            record.
        
        """
        daily_tidemark_objects = self.get_daily_tidemark_objects()
        session.add_all(daily_tidemark_objects)
        session.commit()
        return daily_tidemark_objects