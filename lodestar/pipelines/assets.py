import os
from lodestar.database.maps import asset_map 
from lodestar.database import engine
import pandas as pd
import yfinance as yf
from tqdm import tqdm
import time
import regex as re
import multiprocessing as mp
import datetime as dt
import numpy as np

assets = [a.asset for a in asset_map.values()]


def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

def refresh_assets_table(assets, if_exists):
    full_ticker_info = []
    for i, asset in enumerate(tqdm(assets)):
        ticker_info = get_ticker_info(asset)
        if ticker_info:
            full_ticker_info.append(ticker_info, if_exists)
    try:
        insert_asset_info(full_ticker_info, 'replace')
    except:
        pass
    finally:
        return full_ticker_info

def add_new_asset(asset_symbol:str, if_exists='append'):
    ticker_info = get_ticker_info(asset_symbol)
    insert_asset_info(ticker_info, if_exists)

def insert_asset_info(ticker_info, if_exists='append'):
    df = pd.DataFrame(ticker_info)
    df.columns = [camel_to_snake(c) for c in df.columns]

    df.to_sql(name='assets',
            con=engine,
            schema='landing',
            if_exists=if_exists,
            index=False,
            chunksize=100
    )

def get_ticker_info(asset_symbol:str):
    t = yf.Ticker(asset_symbol)
    time.sleep(1)

    if any(t.info.values()):
        return t.info
    else:
        return None

def get_info_for_tickers(proc_num, assets, return_dict):
    return_dict[proc_num] = [get_ticker_info(a) for a in tqdm(assets)]

if __name__=='__main__':
    mp.freeze_support()
    manager = mp.Manager()
    return_dict = manager.dict()

    jobs = []
    cpu_count = os.cpu_count()
    
    for i in range(cpu_count):
        p = mp.Process(target=get_info_for_tickers,
                       args=(i, assets[i::cpu_count], return_dict)
        )
        jobs.append(p)
        p.start()

    for job in jobs:
        job.join()

    df = pd.DataFrame([r for proc in return_dict.values() for r in proc if r])
    df.columns = [camel_to_snake(c) for c in df.columns]
    df = df.dropna(axis=1, how='all')
    dict_columns = ['sector_weightings', 'holdings', 'bond_holdings', 'bond_ratings', 'equity_holdings']

    df['etl_loaded_datetime_utc'] = dt.datetime.utcnow()
    df.drop(columns=dict_columns).to_sql(name='assets', con=engine, schema='landing', index=False, if_exists='replace')
