import os
import argparse
import numpy as np
import pandas as pd
import datetime as dt

from ... import logger, logging
from ...database.maps import account_map, asset_map 
from ...database.models import TransactionHistory, session
from ...database.functions import on_conflict_do_nothing

account_map = {str(a.account).replace('-',''): a.id for a in account_map.values()}
asset_map = {a.asset.replace(' ','-'): a.id for a in asset_map.values()}

transaction_unique_key = 'transaction_history_api_id_account_id_ref_id_key'

def check_ibkr(filepath):
    p = open(filepath)
    txt = p.read()
    lines = txt.split('\n')
    lines = [l.split(',') for l in lines]
    return any(['Interactive Brokers' in line for line in lines]), lines
      

def check_schwab(filepath):
    p = open(filepath)
    txt = p.read()
    lines = txt.split('\n')
    lines = [l.split(',') for l in lines]
    account = lines[0][0]
    return (account in account_map), lines

if __name__=='__main__':
    from .ibkr import use_cols
    print(use_cols)
    from .schwab import use_cols
    print(use_cols)