import numpy as np

from ..database.models import (AlgoTrading, CurrentBelievability, HighWatermark, LowWatermark, TradeLog
                            #    CurrentBelievability, 
                            #    PriceHistory,
                               )
from ..database.functions import (all_query, 
                                             collection_to_dataframe as to_df)

from ..apis.google import update_sheet

sheet_id='1-r81lqrVvAZxHWRX6n2sSI8im_Be6xA0iCU9YB0LkAg'

# def wtd_avg(df):
#     return (df[['mo_01', 'mo_06', 'yr_01', 'yr_05', 'yr_10','yr_20']] \
#                 * [4,3,2,1,1,1]
#             ).sum(axis=1) / 12

def push_pop_drop_reports(sheet_id: str=sheet_id):
    for sheet_name, db in [('Pop', HighWatermark), 
                            ('Drop', LowWatermark)]:
        print(f"Exporting {sheet_name}.")
        db_df = to_df(all_query(db)).reset_index()[['asset', 'asset_id', 'date', 
                                                    'high_mark', 'day_mvmt', 
                                                    'mo_01', 'mo_06', 'yr_01', 
                                                    'yr_05', 'yr_10','yr_20',
                                                    'watermark']]
        db_df = db_df.fillna(axis=1, method='ffill')
        # db_df['wtd_avg'] = (
        #     db_df[['mo_01', 'mo_06', 'yr_01', 'yr_05', 'yr_10','yr_20']] \
        #         * [4,3,2,1,1,1]
        #     ).sum(axis=1) / 12

        print(f"{sheet_name} shape: {db_df.shape}")
        update_sheet(sheet_id=sheet_id, 
                    sheet_name=sheet_name, 
                    data_object=db_df)
    return db_df

def push_report(sheet_id: str, sheet_name: str, db_object, columns: list = [],
                include_update_time: bool = True, update_time_offset: int = 0):
    records = all_query(db_object)

    df = to_df(records).reset_index()
    if not columns:
        columns = df.columns
    print(f"Exporting {sheet_name} Report.")
    update_sheet(sheet_id=sheet_id, 
                 sheet_name=sheet_name, 
                 data_object=df[columns].fillna('None'), 
                 include_update_time=True,
                 update_time_offset=update_time_offset)
    print(f"{sheet_name} shape: {df[columns].shape}")

def push_trade_report(sheet_id=sheet_id,
                sheet_name='Trade Log',
                db_object=TradeLog,
                columns=['account_id','asset','api','timestamp','side','shares',
                         'avg_price','cost_basis']):
    # TODO: For compatability issues with positions.__init__
    push_report(sheet_id=sheet_id,
                sheet_name='Trade Log',
                db_object=TradeLog,
                columns=['account_id','asset','api','timestamp','side','shares',
                         'avg_price','cost_basis'])

def push_reports(sheet_id: str = sheet_id):
    # push_pop_drop_reports(sheet_id)
    # # Believability Report
    # push_report(sheet_id=sheet_id, 
    #             sheet_name='Believability',
    #             db_object=CurrentBelievability,
    #             columns=['date','asset','believability','confidence'])
    # # Trade Log
    # push_report(sheet_id=sheet_id,
    #             sheet_name='Trade Log',
    #             db_object=TradeLog,
    #             columns=['account_id','asset','api','timestamp','side','shares',
    #                      'avg_price','cost_basis'])
    # AlgoTrading 
    push_report(sheet_id=sheet_id,
                sheet_name='AlgoTrading', 
                db_object=AlgoTrading, 
                columns=['date','asset_id','asset','in_portfolio','trans',
                         'closing_price','bound'],
                update_time_offset=3)

    
if __name__=='__main__':
    push_reports()