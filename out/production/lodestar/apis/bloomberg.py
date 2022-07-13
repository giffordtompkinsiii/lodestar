# Core API https://data.bloomberglp.com/professional/sites/10/2017/03/BLPAPI-Core-Developer-Guide.pdf
# Python Documentaion https://bloomberg.github.io/blpapi-docs/python/3.18/_autosummary/blpapi.Request.html
from lodestar.database.maps import asset_map, tidemark_map
from lodestar import logger

import datetime as dt
import pandas as pd
import blpapi as bb
import time


class BloombergAPI:
    assets = asset_map.values()
    tidemarks = [tm for tm in tidemark_map.values() if not (tm.daily or tm.calculated)]
    partial_dfs = {}

    def __init__(self):
        options = bb.SessionOptions()
        options.setServerHost('localhost')
        options.setServerPort(8194)

        self.api_session = bb.Session(options)
        self.api_session.start()

    def get_tidemarks(self, symbol):
        t = time.time()
        full_df = pd.DataFrame()
        partial_dfs = {}
        for i in range(len(self.tidemarks)//25 + 1):
            partial_df = pd.DataFrame()
            self.api_session.openService('//blp/refdata')
            ref_data_service = self.api_session.getService("//blp/refdata")
            request = ref_data_service.createRequest("HistoricalDataRequest")
            request.getElement('securities').appendValue(f'{symbol} US Equity')

            for tm in self.tidemarks[25 * i: 25 * (i + 1)]:
                request.getElement("fields").appendValue(tm.tidemark.upper())

            request.set("periodicitySelection", "MONTHLY")
            request.set("startDate", "19991231")
            request.set("endDate", dt.date.today().strftime('%Y%m%d'))
            self.api_session.sendRequest(request)
            time.sleep(10)

            while True:
                event = self.api_session.nextEvent(10)
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

        logger.info(f"Total Time: {time.time() - t} seconds.")
        return full_df

