"""
This module imports the daily tidemarks from the excel document.

User specifies a file path with the -p flag.
User can also specify a quarterly pull by using the option -q command.

Steps
-----
1. Import tidemarks.
2. Check for new tidemarks
3. Run through tidemark import by asset and compare to last 20 years for score.
4. Store new values in the database.
5. Pull values for growth tidemarks and store those in database as well.

"""
import argparse
import os
import pandas as pd 
import pickle
import numpy as np 
import datetime as dt
import time
from tqdm import tqdm
import multiprocessing as mp
from sqlalchemy import exists
from data_manager import logger

from ..database.models import Asset, TidemarkHistory, Tidemark, session
from ..database.functions import (all_query, update_database_object, 
                                    collection_to_dataframe as to_df)
# TODO: ensure imported tidemarks are type-confirmed
# from ..database.common_objects import tm_type_map
# TODO: Write page as script and implement argument parsing.
# import argparse 

# Import all valid tidemarks from database and create a mapping dictionary for 
# later use dataframes.
tidemarks = all_query(Tidemark)
tm_map = {
    tm.tidemark: tm for tm in tidemarks
}

# Import all valid assets from database to loop through all procedures.
# Assets must be imported into database before they will be recognized by script.



def prep_tidemark(asset: Asset, tidemark: Tidemark,
                  stack: pd.DataFrame, process: int=0):
    """Format dataframe and upload into database.

    This function takes the stacked dataframe `stack` and filters it by 
    `tidemark.id`. It then calls `update_database_object()` for the 
    TidemarkHistory table and uploads the given `asset`
    and `tidemark` combination.
    """
    stack_by_tidemark = stack[stack.tidemark_id==tidemark.id]

    if not stack_by_tidemark.empty:
        update_database_object(import_df=stack_by_tidemark, 
                           db_records=asset.tidemark_history_qtrly_collection,
                           db_table=TidemarkHistory)
        session.refresh(asset)
    return stack_by_tidemark

def prep_tidemarks(asset: Asset, tidemark: Tidemark, stack: pd.DataFrame, 
                   process: int=0):
    """Format dataframe and upload into database.

    This function takes the stacked dataframe `stack` and filters it by 
    `tidemark.id`. It then calls `update_database_object()` for the 
    TidemarkHistory table and uploads the given `asset`
    and `tidemark` combination.
    """
    values_stack = stack[stack.tidemark_id==tidemark.id]

    if not stack_by_tidemark.empty:
        # Update new values first so scores can pull histories for comparison.
        update_database_object(import_df=values_stack, 
                           db_records=asset.tidemark_history_qtrly_collection,
                           db_table=TidemarkHistory)
        session.refresh(asset)
        # Pull historical values and calculate standard deviation and median for 
        # last 20 years.
        values = to_df(asset.tidemark_history_qtrly_collection).value

        ## Add growth calculation here ?
        meds = values.groupby(level=[0,2])\
                    .rolling(window=80, min_periods=4)\
                    .median()
        stds = values.groupby(level=[0,2])\
                    .rolling(window=80, min_periods=4)\
                    .std()

        for df in [meds, stds]:
            df.index = df.index.map(lambda x: (x[0], x[2][1], x[1]))
            df.index.names = ['asset_id','date','tidemark_id']

        scores = 0.5 + (values - meds) / (2 * 1.382 * stds)
        scores = pd.DataFrame({'score':scores})

        if not scores.empty:
            scores = scores.reset_index()
            scores_stack = scores[scores.tidemark_id==tidemark.id]
        
            update_database_object(import_df=scores_stack, 
                    db_records=asset.tidemark_history_qtrly_collection,
                    db_table=TidemarkHistory)
            session.refresh(asset)
    return values_stack, scores_stack

def prep_asset(asset, workbook, process=0,
               disable_tidemark_pbar=False):
    """Convert dataframe values for given asset into a stack and process.

    Finds the `asset`'s dataframe from the `workbook` and conveerts it innto  longform to pass into the `prep_tidemark()` function. It then calculates `TidemarkHistory.score`s and calls `prep_tidemark()` again.
    """
    dataframe = workbook.get(asset.asset, pd.DataFrame())
    if dataframe.empty \
        or getattr(dataframe.Dates, 'dtype', 
                   np.dtype('O')) in [np.dtype('O'), np.dtype('float64')]:
        logger.log(level=1, msg=f"No data for {asset.asset}.")
        return

    # Set DatetimeIndex to quater-end dates
    try:
        dataframe = dataframe.set_index(
                                dataframe.Dates + pd.offsets.QuarterEnd(n=0)
                            ).drop(columns='Dates')
        dataframe.index.name = 'date'
    except TypeError as e:
        logger.log(level=1, msg=f"{asset.asset} Type Error: {e}")
        return

    # Stack dataframe
    stack = dataframe.melt(ignore_index=False)
    stack['tidemark_id'] = stack.variable.map(
                                    lambda t: getattr(tm_map.get(t), 'id', np.nan)
                                    )
    bad_tidemarks = list(stack.variable[stack.tidemark_id.isna()].unique())
    logger.log(level=1, msg=f'Tidemarks not in database {bad_tidemarks}')
    stack = stack.dropna(subset=['tidemark_id'])
    stack['asset_id'] = asset.id

    # TODO: Combine these two tidemark loops into one function `prep_tidemarks()``
    for tidemark in tqdm(tidemarks, 
                         desc=asset.asset + ' ' * (7 - len(asset.asset)) + 'Tidemarks', 
                         leave=False,
                         disable=disable_tidemark_pbar, 
                         position=3 * process + 1):
        #TODO if tidemark is used for growth calculation prep tidemark specailly.
        prep_tidemark(asset, tidemark, stack, process)

    if not any(asset.tidemark_history_qtrly_collection):
        return
    values = to_df(asset.tidemark_history_qtrly_collection).value
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
            print(f"Error with {asset.asset}:")
            print(df.index)
        except IndexError as ie:
            print(f"Error with {asset.asset}:")
            print(df.head())

    scores = 0.5 + (values - meds) / (2 * 1.382 * stds)
    scores = pd.DataFrame({'score':scores})
    if scores.empty:
        return
        
    scores = scores.reset_index()

    for tidemark in tqdm(tidemarks, 
                         desc=asset.asset + ' ' * (7 - len(asset.asset)) + 'Scores', 
                         disable=disable_tidemark_pbar,
                         position=3 * process + 2, 
                         leave=False):
        prep_tidemark(asset, tidemark, scores, process)

    return dataframe, stack, scores

def daily_import(workbook,
                 process=0, cpu_count=1, start_asset=0, end_asset=None, 
                 test=False, reverse=False, disable_asset_pbar=False, 
                 disable_tidemark_pbar=False):
    assets = all_query(Asset)
    if reverse:
        assets = list(reversed(assets))
    for asset in tqdm(assets[start_asset + process : end_asset : cpu_count], 
                      desc=f'Process {str(process+1).zfill(2)} Assets',
                      disable=disable_asset_pbar,
                      position=3 * process):
        prep_asset(asset, workbook=workbook, process=0, 
                   disable_tidemark_pbar=disable_tidemark_pbar)
    return True


def main():
    """To be run from the command line. Creates and utilizes the Argument Parser."""
    parser = argparse.ArgumentParser(prog='data_manager',
                                    # usage='%(prog)s [options] path',
                                    description=__doc__,
                                    # epilog='for the text shown after the help text'
    )
    parser.add_argument('filepath',
                        help='Run import script with filepath provided as source data. Will use present pickle if none provided.',
                        metavar='absolute_path',
                        action='store',
                        type=str,
    )

    parser.add_argument('-t','--test',
                        help='whether or not this is a test run',
                        action='store_true'
    )

    parser.add_argument('-r','--reverse',
                        help='run import in reverse asset direction.',
                        action='store_true')

    return parser.parse_args()

if __name__=='__main__':

    args = main()
    file_path = args.filepath
    print("file:", file_path)

    workbook = pd.read_excel(file_path, 
                             sheet_name=None, 
                             parse_dates=True, 
                             engine='openpyxl')

    mp.freeze_support()
    jobs = []
    cpu_count = mp.cpu_count()

    if args.test:
        cpu_count = 1

    for i in range(cpu_count):
        p = mp.Process(target=daily_import,
                       args=(workbook, i),
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
