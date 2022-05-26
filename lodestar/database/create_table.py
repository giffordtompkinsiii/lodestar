from lodestar.database.maps import asset_map 
from lodestar.database import engine
import pandas as pd
import yfinance as yf
from tqdm import tqdm
import time
from string import ascii_uppercase
import regex as re

def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()



assets = [a.asset for a in asset_map.values()]

ticker_info = []
for asset in tqdm(assets):
    try:
        t = yf.Ticker(asset)
        time.sleep(1)
    except:
        continue 
    ticker_info.append(t.get_info())

df = pd.DataFrame(ticker_info)
df.columns = [camel_to_snake(c) for c in df.columns]
df.index = df.index + 1

df.to_sql(name='assets',
          con=engine,
          schema='landing',
          if_exists='replace',
          index=True,
          index_label='id'
)