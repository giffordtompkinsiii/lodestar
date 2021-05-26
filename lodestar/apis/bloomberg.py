"""
This module imports the qtrly tidemarks from the excel document.

User specifies a file path with the -p flag.

Steps
-----
1. Import tidemarks.
2. Check for new tidemarks
3. Run through tidemark import by asset and compare to last 20 years for score.
4. Store new values in the database.
5. Pull values for growth tidemarks and store those in database as well.

"""
from lodestar import tidemarks
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

from .. import logger
from ..database.maps import tidemark_map
from ..database.models import Asset, TidemarkHistory, Tidemark, session
from ..database.functions import (all_query, collection_to_dataframe as to_df)

assets = all_query(Asset)

class BloombergPipeline():
    tm_map = {tm.tidemark: tm for tm in tidemark_map.values()}

    def __init__(self, filepath):
        self.filepath = filepath
        self.workbook = pd.read_excel(filepath,
                                      sheet_name=None,
                                      parse_dates=True,
                                      engine='openpyxl')


    class BloombergAsset(Asset):
        def __init__(self, asset:Asset, workbook):
            [setattr(self, k, v) for k,v in asset.__dict__.items()]
            self.dataframe = workbook.get(asset.asset, pd.DataFrame())
            
        def format_dataframe(self):
            """check if dataframe before running."""
            df = self.dataframe

            # Set DatetimeIndex to quater-end dates
            df = df.set_index(df.Dates + pd.offsets.QuarterEnd(n=0)).drop(columns='Dates')
            df.index.name = 'date'

            # Stack dataframe
            stack = df.melt(ignore_index=False)
            stack['tidemark_id'] = stack.variable.map(
                                            lambda t: getattr(BloombergPipeline.tm_map.get(t), 'id', np.nan)
                                            )
            stack = stack.dropna(subset=['tidemark_id'])
            stack['asset_id'] = self.id



    class BloombergTidemark(TidemarkHistory):
        def __init__(self, tm: TidemarkHistory):
            [setattr(self, k, v) for k,v in tm.__dict__.items()]



