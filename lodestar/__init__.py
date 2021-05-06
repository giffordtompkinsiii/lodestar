import logging
import sys
import os
from logging.handlers import TimedRotatingFileHandler

data_file_dir = os.environ['TTG_DATA_DIRECTORY']
FORMATTER = logging.Formatter(
    "%(asctime)s |%(levelname)s| %(module)s: %(lineno)d | %(message)s")
LOG_FILE = os.path.join('logs',f"{__name__}.log")

def get_console_handler():
   console_handler = logging.StreamHandler(sys.stdout)
   console_handler.setFormatter(FORMATTER)
   return console_handler

def get_file_handler():
   file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight')
   file_handler.setFormatter(FORMATTER)
   return file_handler

def get_logger(logger_name, level=None):
   logger = logging.getLogger(logger_name)
   logger.setLevel(level or logging.DEBUG) # better to have too much log than not enough
   logger.addHandler(get_console_handler())
   logger.addHandler(get_file_handler())
   # with this pattern, it's rarely necessary to propagate the error up to parent
   logger.propagate = False
   return logger

logger = get_logger(__name__)