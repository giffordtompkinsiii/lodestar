import os
import sys
import datetime as dt
import logging

data_file_dir = os.environ['TTG_DATA_DIRECTORY']
logger = logging.Logger(__name__)
debug = False