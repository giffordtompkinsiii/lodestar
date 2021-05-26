import os
import argparse
import numpy as np
import pandas as pd
import datetime as dt

from . import logger
from .database.maps import account_map, asset_map 
from .database.models import TransactionHistory, session
from .database.functions import on_conflict_do_nothing

account_map = {a.account: a.id for a in account_map.values()}
asset_map = {a.asset.replace(' ','-'): a.id for a in asset_map.values()}

transaction_unique_key = 'transaction_history_api_id_account_id_ref_id_key'

use_cols = ['Header',
            'DataDiscriminator', 
            'Asset Category',
            'Account', 
            'Symbol', 
            'Date/Time', 
            'Exchange', 
            'Quantity', 
            'T. Price',
            'Proceeds', 
            'Comm/Fee', 
            'Code']

import_cols = ['price',
               'quantity',
               'option',
               'ref_id',
               'expiration_date',
               'strike_price']

def import_csv(filepath):
    p = open(filepath)
    txt = p.read()
    lines = txt.split('\n')
    lines = [l.split(',') for l in lines]
    for i, l in enumerate(lines):
        if l[0] == "Trades":
            break
    return pd.read_csv(filepath, skiprows=i, usecols=use_cols)

def format_transactions(dataframe):
    data_mask = (dataframe.Header=='Data') \
                    & (dataframe.DataDiscriminator=='Order')
    data = dataframe[data_mask].drop(columns=['Header','DataDiscriminator'])
    data['timestamp'] = pd.to_datetime(data['Date/Time'])
    data['account_id'] = data.Account.map(account_map)
    data['asset'] = data.Symbol \
                        .map(lambda a: a.replace('BRK B','BRK-B') \
                                        .split()[0:3])
    data['asset_id'] = data.asset.map(lambda a: asset_map[a[0]]).astype(int)
    data['option'] = data.asset.map(lambda a: len(a) > 1).astype(bool)
    data['expiration_date'] = data.asset.map(lambda a: str(pd.to_datetime(a[1])) \
                                                        if len(a)>1 else np.nan)
    data['strike_price']    = data.asset.map(lambda a: float(a[2]) 
                                                    if len(a) > 1 else np.nan)
    data['price'] = data['T. Price'].str.replace(',','').astype(float)
    data['quantity'] = data['Quantity'].str.replace(',','').astype(float)
    data['exchange'] = data['Exchange']
    data = data.set_index(['account_id','asset_id','timestamp','exchange'])
    data['c'] = data.groupby(level=[0,1,2,3]).rank(method='first').quantity
    data['ref_id'] = data.index.map(lambda s: ':'.join([str(i) for i in s])) \
                         + ":" + data['c'].astype(str)
    trades = data[import_cols].reset_index().itertuples(index=False)
    return [TransactionHistory(api_id=4, **t._asdict()) for t in trades]

def import_transactions(trades):
    return on_conflict_do_nothing(trades, constraint_name=transaction_unique_key)

def main(filepath):
    logger.info(f"Importing IBKR Trades from '{os.path.basename(filepath)}'")
    df = import_csv(filepath)
    transactions = format_transactions(df) 
    return import_transactions(transactions)

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filepath')
    args = parser.parse_args()
    main(args.filepath)