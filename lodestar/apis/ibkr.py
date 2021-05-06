"""Interactive Brokers (IBKR) App Configuration

This module configures the connection to the IBKR Trading Workstation 
or Web Portal and redefines methods therein to work for this program.

Classes
-------
IbkrAccount

References
----------
IBKR's Trading Workstation API Configuration
IBKR TWS API: https://interactivebrokers.github.io/tws-api/introduction.html
IBKR Clients and Wrappers: https://interactivebrokers.github.io/tws-api/client_wrapper.html
IBKR COnnectivity: https://interactivebrokers.github.io/tws-api/connection.html
Research includes: https://realpython.com/python-interface/
"""
import time
import datetime as dt

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.commission_report import CommissionReport
from ibapi.contract import Contract
from ibapi.execution import Execution, ExecutionFilter
from ibapi.order import Order 
from ibapi.order_state import OrderState
from ibapi.account_summary_tags import AccountSummaryTags
from sqlalchemy import exists

from .google import update_sheet

from .. import logger
from ..database.maps import api_map, client_map, account_type_map, account_map
from ..database.functions import all_query, collection_to_dataframe
from ..database.models import (Account, Asset, Client, CurrentPosition, 
                               Position, TradeHistory, session)


# from .reports import push_trade_report
# TODO Move sheet update methods to reports module and import them here.
# Can add method, `func`, to an instance, `a`, by defining the function and 
# using its __get__() method. i.e. `a.func = func.__get__(a)`.
# https://stackoverflow.com/questions/972/adding-a-method-to-an-existing-object-instance

acct_map = {a.account: a for a in account_map.values()}
asset_map = {a.asset: a for a in asset_map.values()}
api_map = {a.api: a for a in api_map.values()}

def str_to_timestamp(datetime_str:str)->dt.datetime:
    date_str, time_str = datetime_str.split('  ')
    date_obj = dt.date(year=int(date_str[0:4]),
                   month=int(date_str[4:6]),
                   day=int(date_str[6:]))
    return dt.datetime.combine(date=date_obj,
                               time=dt.time.fromisoformat(time_str))

class IBapi(EWrapper, EClient):
    """The IBKR App object

    This object combines the Client methods and attributes with the Wrapper 
    object to create an app to be run in `ibkr.py` script.

    All methods are modifications of the EWrapper native methods described 
    on the TWS API reference documentation:
    https://interactivebrokers.github.io/tws-api/interfaceIBApi_1_1EWrapper.html

    Attributes
    ----------
    accounts : dict
        a dictionary of `IbkrAccount` objects keyed by the account's id string

    Methods
    -------
    managedAccounts(accountsList:str):
        Receives a comma-separated string with the managed account ids.
    position(account:str, contract:Contract, position:float, avgCost:float):
        Provides the portfolio's open positions.
    accountSummary(reqId:str, account:str, tag:str, value:str, currency:str):
        Updates the TotalCashValue from the account.
    """

    def __init__(self):
        from ..database.models import session
        EClient.__init__(self, self)
        self.session = session
        self.accounts = {}
        self.initialized = False
        self.debug = False
        self.closing_processed = False

    def check_close(self):
        utc_today = dt.datetime.utcnow()
        closed = (utc_today + dt.timedelta(hours=3)).date() != utc_today

        return closed and self.closing_processed


    def managedAccounts(self, accountsList: str):
        for account in accountsList.split(',')[:-1]:
            try:
                new_account = acct_map[account]
                active_portfolio = new_account.active_portfolio
                if not active_portfolio:
                    active_portfolio = Portfolio()
                setattr(new_account, 'current_positions', {})
                setattr(new_account, 'active_portfolio', active_portfolio)
                self.accounts[account] = new_account
            except KeyError as ke:
                print(f"Account {account} does not exist in database. Please format the new account now.")
                try:
                    new_account = Account()
                    print(f"New Account Found: {account}")
                    for c in client_map.values():
                        print(f"[{c.id}] - {c.client}")
                    new_account.client_id = int(
                        input("Please select a client id number from the list above:\n\t> "))
                    print("Stored new account's client:", 
                          client_map[new_account.client_id])
                    for t in account_type_map.values():
                        print(f"[{t.id}] - {t.type_name}: {t.description}")
                    new_account.type_id = int(
                        input("Please select an account type id number from the list above:\n\t"))
                    print("Stored new account's type:", 
                          account_type_map[new_account.type_id])
                    new_account.alias = ""
                    while new_account.alias == "":
                        new_account.alias = input(f"\nPlease insert an alias for the new account:\n> ")
                    session.add(new_account)
                    session.commit()
                except:
                    print("Formatting of new account failed. Please format in database and restart application.")
                    continue
            print(f"Account: {account} formatted")


    def accountSummary(self, reqId:str, account:str, tag:str, value:str, 
                             currency:str):
        """Updates the TotalCashValue from the account.
        
            * TotalCashValue — Total cash balance recognized at the time of trade + futures PNL

        This method will receive the account information as it appears in 
        the TWS Account Summary Window.

        Specific AccountSummaryTags can be added for expanded capabilities. At this point it is only for the accounts current TotalCashValue.

        Parameters
        ----------
        reqId : Int
            Automatically generated request id
        account : str
            account's id string
        tag : string
            the desired tag (see reference doc for complete list)
        value : string
            the value of the tag for the given account
        currency : string
            the currency for the value of the tag

        See TWS API Reference:
        https://interactivebrokers.github.io/tws-api/interfaceIBApi_1_1EWrapper.html#acd761f48771f61dd0fb9e9a7d88d4f04
        """
        # TODO Convert print to logger.
        print(f"Account: {account} \n\tTag: {tag} \n\tValue: {value} \n\tCurrency:{currency}")

        setattr(self.accounts[account], 'total_cash_value', value)
        setattr(self.accounts[account].active_portfolio, 'cash', float(value))
        self.session.add(self.accounts[account].active_portfolio)
        self.session.commit()
        print(f"Updated total cash value for account {account} - portfolio id: {self.accounts[account].active_portfolio.id}.")


    def accountSummaryEnd(self, reqId=9000):
        print("Account Summary End.")

    def position(self, account: str, contract: Contract, position: float,
                       avgCost: float):
        """Provides the portfolio's open positions.

        Pulls current positions from TWS and calls `.add_position` on the app.

        Feeds data to `EClient.reqPositions()`.

        Parameters
        ----------
        account : str
            the account id string
        contract : Contract
            the ticker for the asset
        pos : float
            the size of the position
        avgCost : float
            average cost of the position (not used in this function)
        """
        print("Position.", "Account:", account, "Symbol:", contract.symbol, 
              "SecType:", contract.secType, "Currency:", contract.currency, 
              "Position:", position, "Avg cost:", avgCost)

        new_position = Position()
        new_position.account_id = self.accounts[account].id  
        try:
            new_position.asset_id = a_map[contract.symbol].id
        except KeyError as ke:
            print(f'Asset "{contract.symbol}" not found. Adding new asset to database.')
            new_asset = Asset()
            new_asset.asset = contract.symbol
            session.add(new_asset)
            session.commit()
            session.refresh(new_asset)
            a_map[contract.symbol] = new_asset
            new_position.asset_id = new_asset.id
        new_position.quantity = position
        new_position.avg_price = avgCost
        new_position.last_modified = dt.datetime.now()
        self.accounts[account].current_positions[contract.symbol] = new_position

        if self.initialized:
            print("App initialized. Running position end procedure.")
            self.position_end_procedure()

    def create_new_portfolio(self, account: Account):
        new_portfolio = Portfolio()
        new_portfolio.account_id = account.id
        new_portfolio.active = True 
        new_portfolio.cash = account.total_cash_value
        self.session.add(new_portfolio)
        self.session.commit()
        for position in account.current_positions.values():
            position.portfolio_id = new_portfolio.id 

        self.session.add_all(account.current_positions.values())
        self.session.commit()
        account.active_portfolio = new_portfolio
        return new_portfolio

    def push_updated_trade_log(self):
        push_trade_report()

    def push_current_positions(self):
        if self.debug:
            return
        sheet_id='1-r81lqrVvAZxHWRX6n2sSI8im_Be6xA0iCU9YB0LkAg'
        sheet_name='Positions'
        positions = all_query(CurrentPosition)
        data = collection_to_dataframe(positions).reset_index()[['asset',
                                                'alias',
                                                'account_id',
                                                'avg_price',
                                                'quantity',
                                                'date',
                                                'cash']]
        print(data.dtypes)
        if self.debug:
            return
        update_sheet(sheet_id=sheet_id, 
                     sheet_name=sheet_name, 
                     data_object=data)

    def position_end_procedure(self):
        print("Starting positionEnd.")
        for a, account in self.accounts.items():
            for position in account.current_positions.values():
                position_mask = lambda p: (position.asset_id == p.asset_id) & \
                                    (position.quantity == p.quantity)

                db_position = next((p for p in account.active_portfolio \
                                                     .positions_collection \
                                   if position_mask(p)), None)
                if db_position:
                    print(f"Matching position for {account.account} - {db_position.assets.asset} - {db_position.quantity}")
                else:
                    print(f"New position. Formatting new portfolio for account {account.account}.")
                    new_portfolio = self.create_new_portfolio(account)
                    print("Sleeping to allow update.")
                    time.sleep(30)
                    break
        print("Pushing current positions to google sheet: https://docs.google.com/spreadsheets/d/1-r81lqrVvAZxHWRX6n2sSI8im_Be6xA0iCU9YB0LkAg/edit?pli=1#gid=1803566426")
        self.push_current_positions()
        print("PositionEnd")

    def positionEnd(self):
        self.position_end_procedure()

    def completedOrder(self, contract:Contract, order:Order, 
                             order_state:OrderState):
        print(contract.symbol,'\n\t','Order:', order,'\n\t', 'OrderState:', order_state.completedTime)

    def exec_details_end_procedure(self):
        print("Starting execDetailsEnd.")
        print("Pushing updated Trade Log to google sheet: https://docs.google.com/spreadsheets/d/1-r81lqrVvAZxHWRX6n2sSI8im_Be6xA0iCU9YB0LkAg/edit?pli=1#gid=1011548667")
        self.push_updated_trade_log()
        print("ExecDetailsEnd")

    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        print("ExecutionId:", execution.execId, "ReqId:", reqId, "Symbol:", contract.symbol, "SecType:", contract.secType, "Time:",execution.time, "AccountNumber:", execution.acctNumber, "Side:", execution.side, "Shares:", execution.shares, "Price:", execution.price, "TotalShares:", execution.cumQty, "AvgPrice:", execution.avgPrice)

        account = self.accounts[execution.acctNumber]

        if any(filter(lambda t: (
                        t.account_id==account.id) & (
                        t.ref_id == execution.execId), 
                      account.trade_history_collection)):
            print("Trade already logged:", execution.execId)
            return
        trade = TradeHistory()
        trade.account_id = account.id
        trade.asset_id = a_map[contract.symbol].id
        trade.api_id = api_map['ibkr'].id
        trade.ref_id = execution.execId # .rsplit('.', maxsplit=1) # Corrections are denoted by id's differing only by the digits after the final period.
        trade.shares = int(execution.shares)
        trade.price = float(execution.price)
        trade.cumulative_shares = int(execution.cumQty)
        trade.avg_price = float(execution.avgPrice)
        trade.total_cash = self.accounts[execution.acctNumber].total_cash_value
        trade.timestamp = str_to_timestamp(execution.time)
        if execution.side == 'BOT':
            trade.sold = False

        print(trade.__dict__)
        self.session.add(trade)
        self.session.commit()

        if self.initialized:
            self.exec_details_end_procedure()
            pass

    def updatePortfolio(self, contract: Contract, position: float,
                        marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL: float,
                        realizedPNL: float, accountName: str):
        print("UpdatePortfolio.", "Symbol:", contract.symbol, "SecType:", 
              contract.secType, "Exchange:", contract.exchange, "Position:", 
              position, "MarketPrice:", marketPrice, "MarketValue:", marketValue, 
              "AverageCost:", averageCost, "UnrealizedPNL:", unrealizedPNL, 
              "RealizedPNL:", realizedPNL, "AccountName:", accountName)

    def updateAccountValue(self, key: str, val: str, currency: str,
                                 accountName: str):
        if key == 'CashBalance':
            print("UpdateAccountValue. Key:", key, "Value:", val,
                "Currency:", currency, "AccountName:", accountName)

    def updateAccountTime(self, timeStamp: str):
        print("UpdateAccountTime. Time:", timeStamp)

    def commissionReport(self, commissionReport: CommissionReport):
        print("CommissionReport.", commissionReport)

    def execDetailsEnd(self, reqId: int):
        print("ExecDetailsEnd. ReqId:", reqId)
        self.exec_details_end_procedure()

def run_ibkr(port: int = 7496, disconnect: bool = False, debug: bool = False):
    app = IBapi()
    logger.info("Attempting to establish connection.")
    app.connect('127.0.0.1', port, 123)
    time.sleep(10) #Sleep interval to allow time for connection to server
    if not app.isConnected():
        connection_msg = "App connection failed. Attempting to establish connection. Be sure to log in to IBKR Desktop App."
        logger.warning(f"\n{'='*len(connection_msg)}\n{connection_msg}\n{'='*len(connection_msg)}\n")
        app.disconnect()
        run_ibkr(port=port, disconnect=disconnect, debug=debug)

    #################
    app.debug = debug
    #################

    #Start the socket in a thread
    print("Starting thread.")
    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()

    time.sleep(10) # Sleep interval to allow time for incoming price data
    if app.isConnected():
        print("Requesting account summary", f"Connection: {app.isConnected()}")
        app.reqAccountSummary(groupName='All', 
                                tags=AccountSummaryTags.TotalCashValue,
                                reqId=9000)
        time.sleep(10)

        print("requesting positions data.")
        app.reqPositions()
        time.sleep(10)

        print("requesting executions data.")
        execution_filter = ExecutionFilter()
        app.reqExecutions(reqId=9000, execFilter=execution_filter)
        time.sleep(10)

        app.reqPnL(reqId=9001, account="U3002184", modelCode="")
        time.sleep(10)
        # TODO: format so that this only runs once called rather than at import.
        # push_reports()

        app.initialized = True
        time.sleep(10)

    if disconnect:
        app.disconnect()

if __name__=='__main__':
    parser  = argparse.ArgumentParser(prog='ibkr')
    parser.add_argument('-p','--port', choices=[4001, 7496],
                        help="Specify the api path by specifying the port: \n\t - 7496 or 'tws' for Trading Workstation. \n\t - 4001 or 'ib' for Interactive Brokers Gateway.",
                        default=7496,
                        type=int)
    parser.add_argument('-d','--disconnect',
                           help="Disconnect when complete. If False (default) app will update the reports with live data.",
                           action='store_true')
    parser.add_argument('-t','--test','--debug',
                        dest='debug',
                        help='Run in test or debug mode. This will not push new reports to the google sheet but will still print out messages from the app.',
                        action='store_true')
    args = parser.parse_args()
    logger.info(f"{args.__dict__}")
    run_ibkr(**args.__dict__)