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

from .. import data_file_dir

from ..database.maps import asset_map, tidemark_map
from ..database.models import Asset, PriceHistory, Tidemark, TidemarkType
from ..database.functions import all_query, collection_to_dataframe as to_df

debug = False

tt_map = {tt.id: tt for tt in all_query(TidemarkType)}
scores_pickle_path = os.path.join(data_file_dir, 'scores_all.pickle')
pickle_path = os.path.join(data_file_dir, 'believability.pickle')

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
        if debug:
            print("No tidemarks.")
        return pd.DataFrame()

    tidemarks_df = to_df(tidemarks_collection)[['value']].unstack('tidemark_id')\
                                                   .droplevel(axis=1, level=0)
    tidemarks_df = tidemarks_df.reorder_levels(['asset_id','date'])
    for d in pd.date_range(start=tidemarks_df.index.levels[1].max(), 
                           end=dt.date.today() + pd.offsets.QuarterEnd(n=0), 
                           freq='Q').values[1:]:
        tidemarks_df = tidemarks_df.reindex(tidemarks_df.index.insert(loc=-1, item=(asset.id, d)))
    tidemarks_df = tidemarks_df.sort_index().fillna(method='ffill')
    if debug:
        print(tidemarks_df.index.names)
        print(tidemarks_df.columns)
    return tidemarks_df.rename(columns=lambda col: tidemark_map[col].tidemark, 
                               level='tidemark_id')

def format_growth_tidemarks(asset: Asset, tidemarks_collection: list, debug: bool = False)->pd.DataFrame:
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
        if debug:
            print("No tidemarks.")
        return pd.DataFrame()

    df = to_df(tidemarks_collection)[['id','value']]
    df['id_value'] = list(zip(df.id, df.value))
    df = df[['id_value']].unstack('tidemark_id') \
                         .rename(columns=lambda col: tidemark_map[col].tidemark, 
                                 level='tidemark_id')
    df = df.reorder_levels(['asset_id','date'])

    return df

def get_scores(dataframe: pd.DataFrame, freq_per_yr: int = 4):
    '''Return the rolling median, standard deviation and scores.
    TODO: make this based on asseet.
    '''
    if dataframe.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    meds = dataframe.rolling(window=freq_per_yr * 20, 
                             min_periods=freq_per_yr)\
                    .median()
    stds = dataframe.rolling(window=freq_per_yr * 20, 
                             min_periods=freq_per_yr)\
                    .std()
    scores = (0.5 + (dataframe - meds) / (2 * 1.382 * stds))
    return meds, stds, scores