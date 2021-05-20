"""This script generates an excel spreadsheet for conducting the Bloomberg Pull.
"""

import os
import time 
import pandas as pd

from string import ascii_uppercase

from .. import logger, data_file_dir, tools_dir
from ..database.maps import asset_map, tidemark_map
from ..database.models import Asset, Tidemark, Bloomberg
from ..database.functions import all_query, collection_to_dataframe as to_df

t_0 = time.time()
default_filepath = os.path.join(tools_dir, 
                                'bloomberg_pull.xls')
vba_project_path = os.path.join(data_file_dir, 'vbaProject.bin')

logger.debug(default_filepath)
logger.debug(vba_project_path)

assets = all_query(Asset)
tidemarks = [tm.tidemark for tm in all_query(Tidemark) \
                    if (not tm.daily) and (not tm.calculated)]
bloombergs = all_query(Bloomberg)
latest_dates = {b.asset_id: b.date for b in all_query(Bloomberg)}

def to_excel_col(integer:int):
    ref = ''
    if integer > 26:
        ref = ref + ascii_uppercase[integer // 26 - 1]
    ref = ref + ascii_uppercase[integer % 26 - 1]
    return  ref

def convert_to_excel(n, d):
    ref, p = '', n
    A = [' ']
    A.extend(ascii_uppercase[0:d])
    while p >= 0:
        p -= 1
        c = int(n//d**p)
        if c or ref:
            ref += A[c - 1]
        n -= c * d**p
    return ref,c,n,p,d

def create_asset_sheet(a: Asset, tidemark_names: list = tidemarks):
    """Generate pd.DataFrames to be passed into an excel workbook writer."""
    tm_columns = ['Dates']
    tm_columns.extend(tidemark_names)

    asset_name = ' '.join([a.asset, 'Equity'])

    try:
        latest_date = a.bloomberg_collection[0].date
        date_str = f"{latest_date.month}/{latest_date.day}/{latest_date.year}"
    except IndexError as ie:
        logger.warning(f"{(a.id, a.asset)} Quarterly Tidemarks are up-to-date")
        return pd.DataFrame()

    bloomberg_formula = f'=@BDH("{asset_name}",' \
                        + f'B1:{to_excel_col(len(tm_columns))}1,' \
                        + f'"{date_str}",TODAY(),' \
                        + f'"Dir=V","Per=Q","Days=A","Dts=S", "Sort=R")'

    logger.info(f"{(a.id, a.asset)} - {bloomberg_formula}")
    
    return pd.DataFrame(
        columns=tm_columns,
        data={'Dates' : [bloomberg_formula]})


def create_workbook(filepath: str = default_filepath):
    sheets = {a.asset: create_asset_sheet(a, tidemarks) \
                for a in sorted(assets, key=lambda a:a.asset)}
    writer = pd.ExcelWriter(path=filepath, 
                            date_format='YYYY-MM-DD', 
                            engine='xlsxwriter')

    for a, df in sheets.items():
        if not df.empty:
            df.to_excel(writer, sheet_name=a, index=False)


    workbook = writer.book
    workbook.filename = filepath

    try:
        workbook.add_vba_project(vba_project_path)
    except UserWarning as uw:
        logger.exception("Run the following terminal command in the directory containing containing `bloomberg_pull.xlsm`. \n$ vba_extract.py bloomberg_pull.xlsm", exc_info=uw)

    # TODO: [ ]
    writer.save()

    # TODO: [ ]
    print(f"Script took {round((time.time() - t_0) / 60.0, 1)} minutes.")

if __name__=='__main__':
    create_workbook()