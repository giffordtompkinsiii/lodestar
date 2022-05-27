# Core API https://data.bloomberglp.com/professional/sites/10/2017/03/BLPAPI-Core-Developer-Guide.pdf
# Python Documentaion https://bloomberg.github.io/blpapi-docs/python/3.18/_autosummary/blpapi.Request.html

import blpapi as bb 
import datetime as dt
import pandas as pd
import time
from sqlalchemy import true
from lodestar import logger
from lodestar.database import engine
from lodestar.database.maps import asset_map, tidemark_map

options = bb.SessionOptions()
options.setServerHost('localhost')
options.setServerPort(8194)

session = bb.Session(options)
session.start() 

# session.openService("//blp/refdata")
# refDataService = session.getService("//blp/refdata")
# request = refDataService.createRequest("ReferenceDataRequest")
# request.append("securities", "IBM US Equity")
# request.append("securities", "/cusip/912828GM6@BGN")
# request.append("fields", "PX_LAST")
# request.append("fields", "DS002")
# session.sendRequest(request, None)

# session.openService("//blp/tasvc")
# tasvcService = session.getService("//blp/tasvc")
# request = tasvcService.createRequest("studyRequest")
# # // set security name
# request.getElement("priceSource").getElement("securityName").setValue("IBM US Equity")
# # // set historical price data
# request.getElement("priceSource").getElement("dataRange").setChoice("historical")
# historicalEle = request.getElement("priceSource").getElement("dataRange").getElement("historical")
# # // set study start date
# historicalEle.getElement("startDate").setValue("20100501")
# # // set study end date
# historicalEle.getElement("endDate").setValue("20210528")
# # // DMI study example - set study attributes
# request.getElement("studyAttributes").setChoice("dmiStudyAttributes")
# dmiStudyEle = request.getElement("studyAttributes").getElement("dmiStudyAttributes")
# # // DMI study interval
# dmiStudyEle.getElement("period").setValue(15)
# # // set historical data price sources for study
# dmiStudyEle.getElement("priceSourceLow").setValue("PX_LOW")
# dmiStudyEle.getElement("priceSourceClose").setValue("PX_LAST")
# session.sendRequest(request, None)


# ## Quarterly Tidemarks
if __name__=='__main__':
    time.sleep(10)
    t = time.time()
    full_df = pd.DataFrame()
    assets = asset_map.values()
    tidemarks = [tm for tm in tidemark_map.values() if not (tm.daily or tm.calculated)]
    partial_dfs = {}

    for i in range(len(tidemarks)//25 + 1):
        partial_df = pd.DataFrame()
        session.openService('//blp/refdata')
        refDataService = session.getService("//blp/refdata")
        request = refDataService.createRequest("HistoricalDataRequest")
        for a in assets:
            request.getElement('securities').appendValue(f'{a.asset} US Equity')

        for tm in tidemarks[25*i:25*(i+1)]:
            request.getElement("fields").appendValue(tm.tidemark.upper())

        request.set("periodicitySelection", "MONTHLY")
        request.set("startDate", "19991231")
        request.set("endDate", dt.date.today().strftime('%Y%m%d'))
        session.sendRequest(request)
        time.sleep(10)
        while True:
            event = session.nextEvent(10)
            if event.eventType()==10:
                break
            logger.info(event.eventType())
            for msg in event:
                logger.info(msg.messageType())
                if msg.messageType()=='HistoricalDataResponse':
                    print(msg.toPy())
                    data = msg.toPy().get('securityData')
                    df = pd.DataFrame(data['fieldData'])
                    df.columns = [c.lower() for c in df.columns]
                    df['security'] = data['security']
                    partial_df = pd.concat([partial_df, df])
                    logger.info(partial_df.shape)
        if not partial_df.empty:
            partial_dfs[i] = partial_df.set_index(['security','date'])

    try:
        full_df = partial_dfs[0].join(partial_dfs[1])
    except KeyError as e:
        logger.warning(e)
        logger.debug(partial_dfs.keys())
    full_df['etl_loaded_datetime_utc'] = dt.datetime.utcnow()
    full_df.reset_index().to_sql(name='tidemark_history', 
                                 con=engine, 
                                 schema='landing',
                                 if_exists='replace', 
                                 index=True, 
                                 index_label='batch_index')
    logger.info(f"Total Time: {time.time() - t} seconds.")
    
