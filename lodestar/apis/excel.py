"""This script generates an excel spreadsheet for conducting the Bloomberg Pull.
"""

import os
import time 
import pandas as pd

from .. import logger
from string import ascii_uppercase
from ..database.models import Asset, Tidemark, Bloomberg  
from ..database.functions import all_query, collection_to_dataframe as to_df

t_0 = time.time()
bloombergs = all_query(Bloomberg)
# Run this line in terminal containing `bloomberg_pull.xlsm`.
# vba_extract.py macro_file.xlsm

def to_excel_col(integer:int):
    ref = ''
    if integer > 26:
        ref = ref + ascii_uppercase[integer // 26 - 1]
    ref = ref + ascii_uppercase[integer % 26 - 1]
    return  ref


latest_dates = {b.asset_id: b.date for b in all_query(Bloomberg)}

assets = all_query(Asset)

tidemarks  = all_query(Tidemark)


def create_asset_sheet(asset: Asset, tidemarks: list):

    tm_columns = ['Dates']
    tm_columns.extend(
        [tm.tidemark for tm in tidemarks \
            if (not tm.daily) and (not tm.calculated)])

    asset_name = ' '.join([asset.asset, 'Equity'])

    try:
        latest_date = asset.bloomberg_collection[0].date
        date_str = f"{latest_date.month}/{latest_date.day}/{latest_date.year}"
    except IndexError as ie:
        return pd.DataFrame()

    bloomberg_formula = f'''=@BDH("{asset_name}",B1:{to_excel_col(len(tm_columns))}1,"{date_str}",TODAY(),"Dir=V","Per=Q","Days=A","Dts=S", "Sort=R")'''
    logger.info(f"{asset.asset} - {bloomberg_formula}")
    
    return pd.DataFrame(
        columns=tm_columns,
        data={'Dates' : [bloomberg_formula]})

sheets = {}
for asset in sorted(assets, key=lambda a:a.asset):
    sheets[asset.asset] = create_asset_sheet(asset, tidemarks)

workbook_filename = os.path.join(os.environ['HOME'],
                                        'Desktop',
                                        'bloomberg_pull.xls')

writer = pd.ExcelWriter(path=workbook_filename, 
                        date_format='YYYY-MM-DD', 
                        engine='xlsxwriter')

for k, v in sheets.items():
    if not v.empty:
        v.to_excel(writer, sheet_name=k, index=False)

workbook = writer.book
workbook.filename = workbook_filename
workbook.add_vba_project('./vbaProject.bin')

writer.save()

print(f"Script took {round((time.time() - t_0) / 60.0, 1)} minutes.")