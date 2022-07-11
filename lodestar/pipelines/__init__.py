import time
import numpy as np
import pandas as pd
import datetime as dt
import yfinance as yf
import multiprocessing as mp

from typing import List

from .. import logging, logger, data_file_dir, tools_dir
from ..database import landing
# from ..database.functions import collection_to_dataframe, on_conflict_do_nothing
from abc import ABC, abstractmethod


class Pipeline(ABC):

    @abstractmethod
    def extract(self) -> dict:
        return dict()

    @abstractmethod
    def transform(self) -> dict:
        return dict()

    @abstractmethod
    def load(self) -> None:
        return None

    # @abstractmethod
    def run_pipeline(self):
        extracted_data = self.extract()
        transformed_data = self.transform(extracted_data)
        self.load(transformed_data)
        print("Pipeline completed")

# TODO: Fix landing module.
# TODO: Create AssetsBulkPipeline?
