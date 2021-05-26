from . import *
from .ibkr import process_history as ibkr_process_history
from .schwab import process_history as schwab_process_history

def process_history(filepath):
    if check_schwab(filepath)[0]:
        schwab_process_history(filepath)
    if check_ibkr(filepath):
        ibkr_process_history(filepath)
    else:
        logger.warning(f"{filepath} is not recognized as IBKR or Schwab Transaction file. Skipping.")

def process_filepath(filepath):
    if os.path.isdir(filepath):
        logger.debug(f"{filepath} is a directory. Searching for files.")
        for _, dirs, files in os.walk(filepath):
            for f in files:
                process_history(os.path.join(filepath, f))
    elif os.path.isfile(filepath):
        process_history(filepath)

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filepath')
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()
    logger.setLevel((args.debug * logging.DEBUG) or logging.INFO)
    process_filepath(args.filepath)