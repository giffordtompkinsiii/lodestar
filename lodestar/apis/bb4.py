from turtle import pos
from tqdm import tqdm
from lodestar.database import engine
from lodestar.database.models import TidemarkHistory, BloombergTidemark, session
from lodestar.database.maps import asset_map, tidemark_map
a_map = {a.asset: a for a in asset_map.values()}
tm_map = {tm.tidemark: tm for tm in tidemark_map.values()}
import datetime as dt
import pandas as pd
import blpapi as bb 

import time

options = bb.SessionOptions()
options.setServerHost('localhost')
options.setServerPort(8194)

bb_session = bb.Session(options)
print(bb_session.start())

date_str = lambda d: d.isoformat().replace('-','')

def process_message(m, asset_id, tidemark_id):
    # print("Message To Py:", m.toPy())
    try:
        security_data = m.toPy()['securityData']
    except:
        # print("No securities data.")
        return None
    asset_name = security_data['security'].split(' ')[0]
    field_data = security_data['fieldData']
    bloomberg_tidemarks = []
    for d in field_data:
        b = {}
        b['asset_id'] = asset_id
        b['date'] = date_str(d.pop('date'))
        _, tm_value = d.popitem()
        b['tidemark_id'] = tidemark_id
        b['tidemark_value'] = tm_value
        # print(b)
        bloomberg_tidemarks.append(b)
    return bloomberg_tidemarks

def process_event(e, asset, tidemark):
    # print("Processing event")
    for msg in event:
        # print("Messsage Type: ",msg.messageType())
        return process_message(msg, asset, tidemark)
        # except:
        #     print(f"EventType not processible: {e.eventType()}")
        #     return None

def process_request(r, asset, tidemark, session, today=dt.date.today()):
    # print("processing request")
    r.getElement("securities").appendValue(f'{asset.asset.upper()} US Equity')
    r.getElement("fields").appendValue(tidemark.tidemark.upper())
    r.set("periodicitySelection", "MONTHLY")
    r.set("startDate", date_str((today - pd.DateOffset(years=20)).date()))
    r.set("endDate", date_str(today))
    session.sendRequest(request)
    return session

if __name__=='__main__':
    i = 4
    bb_session.openService("//blp/refdata")
    refDataService = bb_session.getService("//blp/refdata")
    print("Truncating Bloomberg Tidemarks.")
    # engine.execute('TRUNCATE financial.bloomberg_tidemarks;')
    all_records =[]
    for asset_id, asset in tqdm(list(asset_map.items())[i::8], position=1):
        # print("Asset Name:", asset.asset)
        asset_tidemarks = []
        for tidemark_id, tidemark in tidemark_map.items():
            # print("Tidemark:", tidemark.tidemark)
            request = refDataService.createRequest("HistoricalDataRequest")
            bb_session = process_request(request, asset, tidemark, bb_session)
            event = bb_session.tryNextEvent()
            while event:  
                # print(len(asset_tidemarks))
                new_tidemarks = process_event(event, asset_id, tidemark_id)
                if new_tidemarks:
                    asset_tidemarks.extend(new_tidemarks)
                event = bb_session.tryNextEvent()
                time.sleep(0.5)
        if asset_tidemarks:
            # session.execute(BloombergTidemark.__table__.insert(), asset_tidemarks)
            # all_records.extend(asset_tidemarks)
            insert_query = "INSERT INTO financial.bloomberg_tidemarks (asset_id, tidemark_id, date, tidemark_value) VALUES" + f"""
                {",".join([f"({r['asset_id']}, {r['tidemark_id']}, {r['date']}, {r['tidemark_value']})" for r in asset_tidemarks])}"""
            # print(insert_query)
            engine.execute(insert_query)
            session.commit()
     
            


