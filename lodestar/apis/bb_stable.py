from tqdm import tqdm
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

def process_message(m, asset, tidemark):
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
        b = BloombergTidemark()
        b.asset_id = a_map[asset_name].id
        b.date = d.pop('date')
        tm_name, tm_value = d.popitem()
        b.tidemark_id = tm_map[tm_name.lower()].id
        b.tidemark_value = tm_value
        bloomberg_tidemarks.append(b)
        session.add(b)
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
    bb_session.openService("//blp/refdata")
    refDataService = bb_session.getService("//blp/refdata")
    
    bloomberg_tidemarks_full = []
    for asset in tqdm(asset_map.values()):
        # print("Asset Name:", asset.asset)
        for tidemark in list(tidemark_map.values()):
            # print("Tidemark:", tidemark.tidemark)
            request = refDataService.createRequest("HistoricalDataRequest")
            bb_session = process_request(request, asset, tidemark, bb_session)
            asset_events = []
            event = bb_session.tryNextEvent()
            while event:
                # print("EventType:", event.eventType())
                asset_events.append(event)
                new_bbtm = process_event(event, asset, tidemark)
                if new_bbtm:
                    bloomberg_tidemarks_full.extend(new_bbtm)
                event = bb_session.tryNextEvent()
                # print("sleep")
                time.sleep(1)
        # print("Committing Records")
        session.commit()
     
            


