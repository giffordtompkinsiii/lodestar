"""Interactive Brokers (IBKR) App Configuration

This module configures the connection to the IBKR Trading Workstation 
or Web Portal and redefines methods therein to work for this program.

Classes
-------
IBapi(EWrapper, EClient):
    The IBKR App object

Methods
-------
str_to_timestamp(datetime_str:str) -> dt.datetime:
    Convert IBKR date strings to datetime objects.

References
----------
IBKR's Trading Workstation API Configuration
IBKR TWS API: https://interactivebrokers.github.io/tws-api/introduction.html
IBKR Clients and Wrappers: https://interactivebrokers.github.io/tws-api/client_wrapper.html
IBKR COnnectivity: https://interactivebrokers.github.io/tws-api/connection.html
Research includes: https://realpython.com/python-interface/
"""
import sys
import time
import argparse
import threading
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

from .. import logging, logger, beep
from ..database.maps import (account_map, api_map, asset_map, 
                             client_map, account_type_map as type_map)
from ..database.functions import on_conflict_do_nothing, on_conflict_do_update
from ..database.models import (Account, BalanceHistory, PositionHistory, 
                                TransactionHistory, session)

account_map = {a.account: a for a in account_map.values()}
asset_map = {a.asset: a for a in asset_map.values()}
api_map = {a.api: a for a in api_map.values()}

# IBKR Account Helper Functions
def str_to_timestamp(datetime_str:str)->dt.datetime:
    """Convert IBKR date strings to datetime objects."""
    date_str, time_str = datetime_str.split('  ')
    date_obj = dt.date(year=int(date_str[0:4]),
                    month=int(date_str[4:6]),
                    day=int(date_str[6:]))
    return dt.datetime.combine(date=date_obj,
                            time=dt.time.fromisoformat(time_str))

def request_client_id(account: Account):
    logger.debug("Requesting Client Id.")
    q  = '\n'.join([*[
        f"[{c.id}] - {c.client}" for c in client_map.values()],
        f"\nPlease select a client id number from the list above:\n\t> "])
    try:
        new_client = client_map[int(input(q))]
        account.client_id = new_client.id
        logger.info(f"Stored new account's client: {new_client.client}")
    except KeyError as ke:
        logger.error("Incorrect id selected. Please try again.")
        return request_client_id(account)
    return account

def request_account_type(account: Account):
    q  = '\n'.join([*[
        f"[{t.id}] - {t.type_name}: {t.description}" for t in type_map.values()],
        f"\Please select an type number from the list above:\n\t> "])
    try:
        new_type = type_map[int(input(q))]
        account.type_id = new_type.id
        logger.info(f"Stored new account's type: {new_type.type_name}")
    except:
        logger.error("Incorrect type number selected. Please try again.")
        return request_account_type(account)
    return account

def request_account_alias(account:Account):
    q = f"\nPlease insert an alias for the new account:\n> "
    new_alias = input(q)
    if len(new_alias) > 20:
        logger.warning("Alias must be less than 20 characters. Please try again.")
        return request_account_alias(account)
    account.alias = new_alias
    logger.info(f"Stored new account's alias: {account.alias}")
    return account

def format_new_account(account_name):
    new_account = Account()
    new_account.account = account_name
    logger.info(f"New Account Found: {account_name}")
    # Format the new account's client, type and alias.
    new_account = request_client_id(new_account)
    new_account = request_account_type(new_account)
    new_account = request_account_alias(new_account)

    session.add(new_account)
    session.commit()
    session.refresh(new_account)
    return new_account
        


# Account Summary Value Update Functions
########################################
# Must have arguements: (account:Account, value:float)
# Function to call determined by `tag`.
def update_cash_value(account:Account, value):
    unique_key = 'balance_history_account_id_date_key'
    c = BalanceHistory(account_id=account.id, 
                       balance=value, 
                       date=dt.date.today())
    logger.debug(f"Updating {account.account} total cash value: {value}")
    on_conflict_do_update(c, unique_key)
    return c

tag_func = {}
tag_func['TotalCashValue'] = update_cash_value

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
    accountSummary(reqId:str, account:str, tag:str, value:str, currency:str):
        Updates the TotalCashValue from the account.
    accountSummaryEnd(reqId=9000):
        Notifies when all the accounts' information has been received.
    # TODO: Remove Portfolio() references
    create_new_portfolio(account: Account):
        # REMOVE Portfolio() Reference
    execDetails(reqId: int, contract: Contract, execution: Execution):
        Provides the executions which happened in the last 24 hours.
    execDetailsEnd(reqId: int):
        Indicates the end of the Execution reception.
    managedAccounts(accountsList:str):
        Receives a comma-separated string with the managed account ids.
    position(account:str, contract:Contract, position:float, avgCost:float):
        Provides the portfolio's open positions.
    positionEnd():
        Indicates all the positions have been transmitted.
    push_current_positions():
        Pushes current positions report to gSheet.
    push_updated_trade_log():
        Calls push_trade_report.
    updatePortfolio(contract: Contract, position: float, marketPrice: float, 
                    marketValue: float, averageCost: float, unrealizedPNL: float, 
                    realizedPNL: float, accountName: str):
        Receives the subscribed account's portfolio. 
    updateAccountTime(timeStamp: str):
        Receives the last time on which the account was updated.
    updateAccountValue(key: str, val: str, currency: str, accountName: str):
        Receives the subscribed account's information. 
    """

    trade_history_uk = 'position_history_account_id_api_id_ref_id_key'

    def __init__(self):
        from ..database.models import session
        EClient.__init__(self, self)
        self.accounts = {}
        self.initialized = False
        self.debug = False
        self.market_closed = False
        self.closing = False

        self.account_summary_closed = False
        self.position_closed = False
        self.transactions_closed = False

    def check_close(self):
        """Check if market is closed.
        
        Returns True or False or if IBKR closing procedures have already run."""
        utc_today = dt.datetime.utcnow()
        market_closed = (utc_today + dt.timedelta(hours=3)).date() != utc_today
        if not market_closed:
            logger.info("Market is not closed.")
        else:
            logger.info("Market is closed.")
        return market_closed

    def managedAccounts(self, accountsList: str):
        """Receives a comma-separated string with the managed account ids. 
        
        Occurs automatically on initial API client connection.
        """
        for account_name in accountsList.split(',')[:-1]:
            try:
                account = account_map[account_name]
            except KeyError as ke:
                logger.warning(f"Account {account_name} does not exist in database. Please format the new account now.")
                account = format_new_account(account_name)
            account.pending_trades = []
            self.accounts[account_name] = account
            logger.debug(f"Account: {account.account} formatted.")


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
        logger.debug(f"Account: {account} Tag: {tag} "
                     + f"Value: {value} Currency:{currency}")
        account = self.accounts[account]
        tag_func[tag](account, value)
        return value

    def accountSummaryEnd(self, reqId=9000):
        """Notifies when all the accounts' information has been received."""
        logger.info("Account Cash Balances Updated.")
        if self.closing:
            self.account_summary_closed = True

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
        logger.debug(f"Position Details -- Account: {account}, "
                    + f"Symbol: {contract.symbol}, SecType: {contract.secType}, "
                    + f"Currency: {contract.currency}, Position: {position}, "
                    + f"Avg cost: {avgCost}")
        if contract.secType != 'STK':
            return
        account = self.accounts[account]
        b_id = account.current_balance.id
        p = PositionHistory(balance_id=b_id,
                            asset_id=asset_map[contract.symbol].id,
                            api_id=api_map['ibkr'].id,
                            position=position)
        logger.debug(f"Adding PositionHistory({p.__dict__}) to database")
        on_conflict_do_update(p, 'position_history_balance_id_asset_id_key')

    def positionEnd(self):
        """Indicates all the positions have been transmitted."""
        logger.info("Current Positions Updated.")
        if self.closing:
            self.position_closed = True

    def execDetailsEnd(self, reqId: int):
        """Indicates the end of the Execution reception."""
        for a in self.accounts.values():
            a.pending_trades = on_conflict_do_nothing(a.pending_trades, 
                                            constraint_name=self.trade_history_uk)
        logger.info("Transactions Updated")
        if self.closing:
            self.transactions_closed = True

    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        """Provides the executions which happened in the last 24 hours."""
        logger.debug(f"ExecutionId: {execution.execId}, ReqId: {reqId}, " 
            + f"Symbol: {contract.symbol}, SecType:, {contract.secType}, "
            + f"Time: {execution.time}, AccountNumber: {execution.acctNumber}, "
            + f"Side: {execution.side}, Shares: {execution.shares}, "
            + f"Price: {execution.price}, TotalShares: {execution.cumQty}, "
            + f"AvgPrice: {execution.avgPrice}")
        ref_id_base = execution.execId.rsplit('.', maxsplit=1)[0]
        account = self.accounts[execution.acctNumber]
        sign = (-1 + 2 * (execution.side=='BOT'))
        trade = TransactionHistory(account_id=account.id,
                                   timestamp=str_to_timestamp(execution.time),
                                   asset_id=asset_map[contract.symbol].id,
                                   api_id=api_map['ibkr'].id,
                                   ref_id=ref_id_base,
                                   exchange=execution.exchange,
                                   strike_price=contract.strike,
                                #    expiration_date= NULL
                                   option=(contract.secType!='STK'),
                                   price=execution.price,
                                   quantity=sign * execution.shares)
        logger.debug(f"Adding {trade.__dict__} to session.")
        account.pending_trades.append(trade)

    def closing_procedures(self):
        """Running closing procedures."""
        logger.info("Initializing closing procedure.")
        self.closing = True
        self.reqPositions()
        self.reqAccountSummary(reqId=9000, groupName='All',     
                               tags=AccountSummaryTags.TotalCashValue)
        self.reqExecutions(reqId=9000, execFilter=ExecutionFilter())
        while not all([self.transactions_closed,
                      self.position_closed,
                      self.account_summary_closed]):
            time.sleep(10)
        logger.info("App is disconnecting.")
        self.disconnect()

    def check_connection_or_wait(self, ip, port, client_id):
        if not self.isConnected():
            connection_msg = "App connection failed. " \
                            + "Attempting to establish connection. " \
                            + "Be sure to log in to IBKR Desktop App."
            beep()
            logger.warning(f"\n{'='*len(connection_msg)}\n{connection_msg}\n{'='*len(connection_msg)}\n")

            input("Press Enter after logging in, "
                    + "or press `Ctrl+C` to disconnect.")
            self.connect(ip, port, client_id)
            time.sleep(10)
            self.check_connection_or_wait(ip, port, client_id)
        

def run_ibkr(port: int = 7496, disconnect: bool = False, debug: bool = False):
    app = IBapi()
    logger.info("Establishing connection.")
    app.connect('127.0.0.1', port, 124)
    time.sleep(10) #Sleep interval to allow time for connection to server
    app.check_connection_or_wait('127.0.0.1', port, 124)

    #################
    logger.setLevel((debug * logging.DEBUG) or logging.INFO)
    #################

    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()

    time.sleep(10) # Sleep interval to allow time for incoming price data
    if app.isConnected():
        logger.info("App is requesting `Account Balance Data`")
        app.reqAccountSummary(groupName='All', 
                                tags=AccountSummaryTags.TotalCashValue,
                                reqId=9000)
        time.sleep(10)

        logger.info("App is requesting `Positions Data`")
        app.reqPositions()
        time.sleep(10)

        logger.info("App is requesting `Executions Data`")
        execution_filter = ExecutionFilter()
        app.reqExecutions(reqId=9000, execFilter=execution_filter)
        time.sleep(10)

        app.initialized = True
        time.sleep(10)

    while not app.market_closed:
        app.market_closed = app.check_close()
        time.sleep(2)

    app.closing_procedures()

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
    logger.debug(f"{args.__dict__}")
    run_ibkr(**args.__dict__)