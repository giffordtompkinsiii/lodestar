import logging
import sys
import os
from logging.handlers import TimedRotatingFileHandler
# TODO: remove these and set-up default variables.
tools_dir = "C:\\Users\\Leon Tompkins\\tools" #os.environ['TTG_TOOLS_DIRECTORY']
data_file_dir = "C:\\Users\\Leon Tompkins\\tools" #os.environ['TTG_DATA_DIRECTORY']

log_format = "%(asctime)s |%(levelname)s| %(module)s: %(lineno)d | %(message)s"
formatter = logging.Formatter(log_format)
## TODO: Add datadirectory to filepath for production version
log_file = os.path.join('logs',f"{__name__}.log")

def beep():
    os.system('afplay /System/Library/Sounds/Sosumi.aiff')

def get_console_handler():
   console_handler = logging.StreamHandler(sys.stdout)
   console_handler.setFormatter(formatter)
   return console_handler

def get_file_handler():
   file_handler = TimedRotatingFileHandler(log_file, when='midnight')
   file_handler.setFormatter(formatter)
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