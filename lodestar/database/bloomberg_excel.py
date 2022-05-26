from numpy import full
import pandas as pd
from lodestar.database import engine
from lodestar.database.maps import asset_map

assets = [a.asset for a in asset_map.values()]

asset_dataframes = pd.read_excel('~/Downloads/20_MAY_2021.xlsm',
              sheet_name=None
)

full_data = []
for asset, df in asset_dataframes.items():
    df.symbol = asset 
    full_data.append(df)

df = pd.concat(full_data).reset_index()
df.index = df.index + 1

df.to_sql('tidemark_history',
    con=engine,
    schema='landing',
    index=True,
    index_label='id',
    if_exists='replace')