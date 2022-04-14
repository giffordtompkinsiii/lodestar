"""
This module imports the qtrly tidemarks from the excel document.

User specifies a `filepath`.

Steps
-----
1. Import tidemarks.
2. Check for new tidemarks
3. Run through tidemark import by asset and compare to last 20 years for score.
4. Store new values in the database.
5. Pull values for growth tidemarks and store those in database as well.

"""
import os
import time
import pickle
import argparse

import numpy as np 
import pandas as pd 
import datetime as dt
import multiprocessing as mp

from tqdm import tqdm
from sqlalchemy import exists

from ..pipelines.tidemarks.growth import calculate_growth_tidemarks

from .. import logger, data_file_dir
from ..database.maps import asset_map, tm_name_id_map
from ..database.models import Asset, Bloomberg, TidemarkHistory, Tidemark, session
from ..database.functions import (all_query, update_database_object, 
                                         collection_to_dataframe)

class ExcelPipeline(object):
    def __init__(self, asset, dataframe):
        self.asset = asset
        self.dataframe = dataframe
        pass

    def prep_asset(self, process=0,
                disable_tidemark_pbar=False):
        """Convert dataframe values for given asset into a stack and process.

        Finds the `asset`'s dataframe from the `workbook` and conveerts it innto  longform to pass into the `prep_tidemark()` function. It then calculates `TidemarkHistory.score`s and calls `prep_tidemark()` again.
        """
        a, df = self.asset, self.dataframe
        if df.empty \
            or getattr(df.Dates, 'dtype', 
                    np.dtype('O')) in [np.dtype('O'), np.dtype('float64')]:
            logger.log(level=1, msg=f"No data for {a.asset}.")
            return

        # Set DatetimeIndex to quater-end dates
        try:
            df = df.set_index(df.Dates + pd.offsets.QuarterEnd(n=0)) \
                   .drop(columns='Dates')
            df.index.name = 'date'
        except TypeError as e:
            logger.log(level=1, msg=f"{a.asset} Type Error: {e}")
            return

        # Stack dataframe
        stack = df.melt(ignore_index=False)
        stack['tidemark_id'] = stack.variable.map(
                                        lambda t: getattr(tm_map.get(t), 'id', np.nan)
                                        )
        bad_tidemarks = list(stack.variable[stack.tidemark_id.isna()].unique())
        logger.log(level=1, msg=f'Tidemarks not in database {bad_tidemarks}')
        stack = stack.dropna(subset=['tidemark_id'])
        stack['asset_id'] = a.id

        # TODO: Combine these two tidemark loops into one function `prep_tidemarks()``
        for tidemark in tqdm(tidemarks, 
                            desc=a.asset + ' ' * (7 - len(a.asset)) + 'Tidemarks', 
                            leave=False,
                            disable=disable_tidemark_pbar, 
                            position=3 * process + 1):
            #TODO if tidemark is used for growth calculation prep tidemark specailly.
            prep_tidemark(a, tidemark, stack, process)

        if not any(a.tidemark_history_qtrly_collection):
            return
        values = collection_to_dataframe()(a.tidemark_history_qtrly_collection).value
        values = values.reorder_levels([0,2,1]).sort_index()
        meds = values.groupby(level=[0,1])\
                    .rolling(window=80, min_periods=4)\
                    .median()
        stds = values.groupby(level=[0,1])\
                    .rolling(window=80, min_periods=4)\
                    .std()

        for df in [meds, stds]:
            try:
                df.index = values.index
            except TypeError as te:
                print(f"Error with {a.asset}:")
                print(df.index)
            except IndexError as ie:
                print(f"Error with {a.asset}:")
                print(df.head())

        scores = 0.5 + (values - meds) / (2 * 1.382 * stds)
        scores = pd.DataFrame({'score':scores})
        if scores.empty:
            return
            
        scores = scores.reset_index()

        for tidemark in tqdm(tidemarks, 
                            desc=a.asset + ' ' * (7 - len(a.asset)) + 'Scores', 
                            disable=disable_tidemark_pbar,
                            position=3 * process + 2, 
                            leave=False):
            prep_tidemark(a, tidemark, scores, process)

        return df, stack, scores


class BloombergPipeline(object):
    def __init__(self, filepath):
        self.workbook = pd.read_excel(filepath, 
                                      sheet_name=None, 
                                      parse_dates=True, 
                                      engine='openpyxl')

    def get_workbook_sheets(self):
        for asset, dataframe in self.workbook.items():
            e = ExcelPipeline(asset, dataframe)

    def process_workbook(self, max_processes: int = None):
        mp.freeze_support()
        jobs = []
        cpu_count = max_processes or mp.cpu_count()

        for i in range(cpu_count):
            p = mp.Process(target=daily_import,
                        args=(self.workbook, i),
                        kwargs={
                            'cpu_count': cpu_count,
                            'start_asset': 0,
                            'end_asset': None,
                            'test':False, 
                            'disable_asset_pbar':False,
                            'disable_tidemark_pbar':False,
                            'reverse': args.reverse})
            jobs.append(p)
            p.start()

        for job in jobs:
            job.join()



if __name__=='__main__':
    b = BloombergPipeline()
    b.generate_workbook()