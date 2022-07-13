from lodestar.database.models import AlgoTrading, HighWatermark, LowWatermark, TradeLog
from lodestar.database.functions import all_query, collection_to_dataframe as to_df

from lodestar.apis.google import update_sheet

portfolio_sheet_id = '1-r81lqrVvAZxHWRX6n2sSI8im_Be6xA0iCU9YB0LkAg'


def push_pop_drop_reports(sheet_id: str = portfolio_sheet_id):
    for sheet_name, db in [('Pop', HighWatermark),
                           ('Drop', LowWatermark)]:
        print(f"Exporting {sheet_name}.")
        db_df = to_df(all_query(db)).reset_index()[['asset', 'asset_id', 'date',
                                                    'high_mark', 'day_mvmt',
                                                    'mo_01', 'mo_06', 'yr_01',
                                                    'yr_05', 'yr_10', 'yr_20',
                                                    'watermark']]
        db_df = db_df.fillna(axis=1, method='ffill')

        print(f"{sheet_name} shape: {db_df.shape}")
        update_sheet(sheet_id=sheet_id,
                     sheet_name=sheet_name,
                     data_object=db_df)
    return db_df


def push_report(sheet_id: str, sheet_name: str, db_object, columns: list = [], update_time_offset: int = 0):
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


def push_reports(sheet_id: str = portfolio_sheet_id):
    push_report(sheet_id=sheet_id,
                sheet_name='AlgoTrading',
                db_object=AlgoTrading,
                columns=['date', 'asset_id', 'asset', 'believability', 'in_portfolio', 'transaction',
                         'closing_price', 'trigger_price'],
                update_time_offset=3)


if __name__ == '__main__':
    push_reports()
