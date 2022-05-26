# Core API https://data.bloomberglp.com/professional/sites/10/2017/03/BLPAPI-Core-Developer-Guide.pdf
# Python Documentaion https://bloomberg.github.io/blpapi-docs/python/3.18/_autosummary/blpapi.Request.html

import blpapi as bb 
import datetime as dt
from lodestar.database.maps import asset_map, tidemark_map

options = bb.SessionOptions()
options.setServerHost('192.168.100.1')
options.setServerPort(8194)

session = bb.Session(options)
session.start() 

session.openService("//blp/refdata")
refDataService = session.getService("//blp/refdata")
request = refDataService.createRequest("ReferenceDataRequest")
request.append("securities", "IBM US Equity")
request.append("securities", "/cusip/912828GM6@BGN")
request.append("fields", "PX_LAST")
request.append("fields", "DS002")
session.sendRequest(request, None)

session.openService("//blp/tasvc")
tasvcService = session.getService("//blp/tasvc")
request = tasvcService.createRequest("studyRequest")
# // set security name
request.getElement("priceSource").getElement("securityName").setValue("IBM US Equity")
# // set historical price data
request.getElement("priceSource").getElement("dataRange").setChoice("historical")
historicalEle = request.getElement("priceSource").getElement("dataRange").getElement("historical")
# // set study start date
historicalEle.getElement("startDate").setValue("20100501")
# // set study end date
historicalEle.getElement("endDate").setValue("20210528")
# // DMI study example - set study attributes
request.getElement("studyAttributes").setChoice("dmiStudyAttributes")
dmiStudyEle = request.getElement("studyAttributes").getElement("dmiStudyAttributes")
# // DMI study interval
dmiStudyEle.getElement("period").setValue(15)
# // set historical data price sources for study
dmiStudyEle.getElement("priceSourceLow").setValue("PX_LOW")
dmiStudyEle.getElement("priceSourceClose").setValue("PX_LAST")
session.sendRequest(request, None)


# ## Quarterly Tidemarks
if __name__=='__main__':
# tidemarks = ["RETURN_COM_EQY", 
#              "RETURN_ON_WORK_CAP",
#              "RETURN_ON_INV_CAPITAL"]
    session.openService('//blp/refdata')
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("HistoricalDataRequest")
    for a in asset_map.values():
        request.getElement('securities').appendValue(f'{a.asset} US Equity')

    for tm in tidemark_map.values():
        request.getElement("fields").appendValue(tm.tidemark.upper())

    # request.getElement("securities").appendValue('IBM US Equity')
    # request.getElement("securities").appendValue('AAPL US Equity')
    # request.getElement("securities").appendValue('NUE US Equity')

    # request.getElement("fields").appendValue(tidemarks[0])
    # request.getElement("fields").appendValue(tidemarks[1])
    # request.getElement("fields").appendValue(tidemarks[2])

    request.set("periodicitySelection", "MONTHLY")
    request.set("startDate", "19991231")
    request.set("endDate", dt.date.today().strftime('%Y%m%d'))
    session.sendRequest(request)

    event = session.nextEvent()
    event.eventType()
    for msg in event:
        print(msg.messageType())