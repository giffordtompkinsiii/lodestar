from . import *

use_cols = ['Settlement Date',
            'Action', 
            'Symbol/CUSIP',
            'Quantity', 
            'Price', 
            'Amount', 
            'Fee/Comm', 
            'Security Type']

import_cols = ['price',
               'quantity',
               'ref_id']

def import_csv(filepath):
    is_schwab, lines = check_schwab(filepath)
    if not is_schwab:
        logger.warning(f"{filepath} is not an Schwab Transaction Statement. Please use appropriate processing function.")
        return pd.DataFrame()
    logger.debug("Found Schwab statement.")
    for i, l in enumerate(lines):
        if l[0] == "Date":
            break
    df = pd.read_csv(filepath, skiprows=i, usecols=use_cols)
    df['account'] = lines[0][0]
    return df

def format_transactions(dataframe):
    data_mask = (dataframe.Action.isin(['Buy','Sell']))
    data = dataframe[data_mask]
    data['timestamp'] = pd.to_datetime(data['Settlement Date'])
    data['account_id'] = data.account.map(account_map).astype(int)
    data['asset'] = data['Symbol/CUSIP'] \
                        .map(lambda a: a.replace('BRK B','BRK-B').strip())
    data['asset_id'] = data.asset.map(lambda a: asset_map[a]).astype(int)
    data['price'] = (data.Price.replace('[\$,)]', '', regex=True)
                               .replace('[(]', '-', regex=True).astype(float))
    data['quantity'] = data['Quantity'].str.replace(',','').astype(float)
    data = data.set_index(['account_id','asset_id','timestamp'])
    data['c'] = data.groupby(level=[0,1,2]).rank(method='first').quantity
    data['ref_id'] = data.index.map(lambda s: ':'.join([str(i) for i in s])) \
                         + ":" + data['c'].astype(str)
    trades = data[import_cols].reset_index().itertuples(index=False)
    return [TransactionHistory(api_id=3, **t._asdict()) for t in trades]

def import_transactions(trades):
    return on_conflict_do_nothing(trades, constraint_name=transaction_unique_key)

def process_history(filepath):
    logger.info(f"Importing Schwab Trades from '{os.path.basename(filepath)}'")
    df = import_csv(filepath)
    if df.empty:
        return df
    transactions = format_transactions(df) 
    return import_transactions(transactions)

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filepath')
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()
    logger.setLevel((args.debug * logging.DEBUG) or logging.INFO)
    process_history(args.filepath)