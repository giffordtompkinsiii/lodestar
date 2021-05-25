import time
import numpy as np
import pandas as pd
import datetime as dt
import yfinance as yf
import multiprocessing as mp

from typing import List

from .. import logging, logger, data_file_dir, tools_dir
from ..database.maps import asset_map
from ..database.models import Asset, BuoyHistory, PriceHistory, session
from ..database.functions import collection_to_dataframe, on_conflict_do_nothing

class AssetPipeline(object):
    today = dt.date.today()
    end_of_day = (dt.datetime.utcnow() + dt.timedelta(hours=3)).date()
    date_21y_ago = pd.to_datetime(dt.date(year=today.year - 21, 
                                          month=today.month, 
                                          day=today.day))
    logger.debug(f"End of day: {end_of_day}")
    logger.debug(f"20 years ago: {date_21y_ago}")

    def __init__(self, asset:Asset, debug:bool = False):
        self.asset = asset 
        self.debug = debug 
        logger.setLevel((debug * logging.DEBUG) or logging.INFO)