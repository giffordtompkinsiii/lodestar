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

from ..tidemarks.growth import calculate_growth_tidemarks

from .. import logger
from ..database.maps import asset_map, tm_name_id_map
from ..database.models import Asset, TidemarkHistory, Tidemark, session
from ..database.functions import (all_query, update_database_object, 
                                         collection_to_dataframe)

# Import all valid tidemarks from database and create a mapping dictionary for 
# later use dataframes.
tidemarks = all_query(Tidemark)

assets = all_query(Asset)

def process_tidemarks(asset: Asset, stack: pd.DataFrame) -> Asset:
    """Format dataframe and upload into database.

    This function takes the stacked dataframe `stack` and filters it by 
    `tidemark.id`. It then calls `update_database_object()` for the 
    TidemarkHistory table and uploads the given `asset`
    and `tidemark` combination.
    """
    # This history is in database long-format
    # (id, asset_id, date, tidemark_id, value, med_20y, std_20y, score)
    tm_history = collection_to_dataframe(asset.tidemark_history_collection)
    tm_history = tm_history.reorder_levels(['asset_id','tidemark_id','date']) \
                           .sort_index()

    stack = stack.set_index(['tidemark_id','asset_id'], append=True) \
                 .reorder_levels(['asset_id','tidemark_id','date']) \
                 .sort_index()[['value']]
    import_df = tm_history.combine_first(stack)

    growth_tm = calculate_growth_tidemarks(asset, import_df)
    import_df = import_df.combine_first(growth_tm)

    grouped_df = import_df.reset_index('date') \
                          .groupby(level=[0,1]) \
                          .rolling(window=80, min_periods=4, on='date')

    import_df['std_20y'] = grouped_df.std() \
                                     .set_index('date', append=True)['value']

    import_df['med_20y'] = grouped_df.median() \
                                     .set_index('date', append=True)['value']

    # scores = 0.5 + (values - meds) / (2 * 1.382 * stds)
    import_df['score'] = 0.5 + ((import_df.value - import_df.med_20y) \
                            / (2 * 1.382 * import_df.std_20y))

    return import_df


def format_tidemarks(asset, workbook):
    """Format tidemarks for prep_tidemarks function"""    
    dataframe = workbook.get(asset.asset, pd.DataFrame())
    if dataframe.empty \
        or getattr(dataframe.Dates, 'dtype', 
                   np.dtype('O')) in [np.dtype('O'), np.dtype('float64')]:
        logger.log(level=1, msg=f"No data for {asset.asset}.")
        return pd.DataFrame()

    # Set DatetimeIndex to quater-end dates
    try:
        dataframe = dataframe.set_index(
                                dataframe.Dates + pd.offsets.QuarterEnd(n=0)
                            ).drop(columns='Dates')
        dataframe.index.name = 'date'
    except TypeError as e:
        logger.log(level=1, msg=f"{asset.asset} Type Error: {e}")
        return pd.DataFrame()

    # Stack dataframe
    stack = dataframe.melt(ignore_index=False)
    stack['tidemark_id'] = stack.variable.map(tm_name_id_map)
    stack = stack.dropna(subset=['tidemark_id'])
    stack['asset_id'] = asset.id

    return stack


def process_asset(asset, workbook, process=0, disable_tidemark_pbar=False):
    """Convert dataframe values for given asset into a stack and process.

    Finds the `asset`'s dataframe from the `workbook` and conveerts it into 
    longform to pass into the `prep_tidemark()` function. It then calculates 
    `TidemarkHistory.score`s and calls `prep_tidemark()` again.

    Parameters
    ----------
    asset: Asset
    workbook: dict(asset_name: pd.DataFrame)
        A dictionary of asset keys and dataframes imported from an excel workbook.
    process: int
    disable_tidemark_pbar: bool

    Returns
    -------
    None
    """
    stack = format_tidemarks(asset, workbook)
    import_df = process_tidemarks(asset, stack)
    return import_df
    # return update_database_object(import_df=import_df, 
    #                             db_records=asset.tidemark_history_collection,
    #                             db_table=TidemarkHistory,
    #                             refresh_object=asset)

def excel_import(workbook, process=0, cpu_count=1, start_asset=0, end_asset=-1, 
                 test=False, disable_asset_pbar=False, 
                 disable_tidemark_pbar=False):
    """Formats and import new data from excel notebook to database."""
    for asset in tqdm(assets[start_asset + process : end_asset : cpu_count], 
                      desc=f'Process {str(process + 1).zfill(2)} Assets',
                      disable=disable_asset_pbar,
                      position=3 * process):
        asset = process_asset(asset, workbook=workbook, process=0, 
                              disable_tidemark_pbar=disable_tidemark_pbar)
    return True


# def main():
#     """To be run from the command line. Creates and utilizes the Argument Parser."""
#     parser = argparse.ArgumentParser(prog='data_manager',
#                                     # usage='%(prog)s [options] path',
#                                     description=__doc__,
#                                     # epilog='for the text shown after the help text'
#     )
#     parser.add_argument('filepath',
#                         help='Run import script with filepath provided as source data. Will use present pickle if none provided.',
#                         metavar='absolute_path',
#                         action='store',
#                         type=str,
#     )

#     parser.add_argument('-c','--cpu-count',
#                         help='max number of cpus to use for database update procedures',
#                         default=None,
#                         dest='cpu_count',
#                         type=int,
#     )

#     parser.add_argument('-t','--test',
#                         help='whether or not this is a test run',
#                         action='store_true'
#     )

#     return parser.parse_args()

# # if __name__=='__main__':

#     args = main()
#     print("file:", args.filepath)

#     workbook = pd.read_excel(args.filepath, 
#                              sheet_name=None, 
#                              parse_dates=True, 
#                              engine='openpyxl')

#     mp.freeze_support()
#     jobs = []
#     cpu_count = args.cpu_count or mp.cpu_count()

#     if args.test:
#         cpu_count = 1

#     for i in range(cpu_count):
#         p = mp.Process(target=excel_import,
#                        args=(workbook, i),
#                        kwargs={
#                            'cpu_count':-cpu_count,
#                            'start_asset': -1 - cpu_count,
#                            'end_asset':0,
#                            'test':False, 
#                            'disable_asset_pbar':False,
#                            'disable_tidemark_pbar':False})
#         jobs.append(p)
#         p.start()

#     for job in jobs:
#         job.join()


if __name__=='__main__':
    asset = asset_map[596]
    filepath1 = '~/Downloads/7_APR_2021.xlsm'
    workbook1 = pd.read_excel(filepath1, 
                            sheet_name=None, 
                            parse_dates=True, 
                            engine='openpyxl')


    import_df1 = process_asset(asset, workbook1)

    filepath = '~/Downloads/5_APR_2021.xlsm'
    workbook = pd.read_excel(filepath, 
                            sheet_name=None, 
                            parse_dates=True, 
                            engine='openpyxl')


    import_df = process_asset(asset, workbook)




