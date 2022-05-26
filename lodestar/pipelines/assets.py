from multiprocessing.context import assert_spawning
from numpy import insert
from lodestar.database.maps import asset_map 
from lodestar.database import engine
import pandas as pd
import yfinance as yf
from tqdm import tqdm
import time
import regex as re

assets = [a.asset for a in asset_map.values()]


def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

def refresh_assets_table():
    full_ticker_info = []
    for i, asset in enumerate(tqdm(assets[0:200])):
        ticker_info = get_ticker_info(asset)
        full_ticker_info.append(ticker_info)
    insert_asset_info(full_ticker_info, 'replace')

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
            index=False
    )

def get_ticker_info(asset_symbol:str):
    t = yf.Ticker(asset_symbol)
    time.sleep(1)

    if any(t.info.values()):
        return t.info
    else:
        return None

def main():
    for i, asset in enumerate(tqdm(assets)):
        if i:
            add_new_asset(asset)
        else:
            add_new_asset(asset, 'replace')

if __name__=='__main__':
    refresh_assets_table()