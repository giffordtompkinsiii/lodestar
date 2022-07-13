import datetime as dt
import yfinance as yf
from sqlalchemy.orm.session import sessionmaker
from pipelines import Pipeline
from lodestar import logging, logger
from lodestar.database import landing, engine


class AssetPipeline(Pipeline):
    # today = dt.date.today()
    # end_of_day = (dt.datetime.utcnow() + dt.timedelta(hours=3)).date()
    # date_21y_ago = pd.to_datetime(dt.date(year=today.year - 21, 
    #                                       month=today.month, 
    #                                       day=today.day))
    # logger.debug(f"End of day: {end_of_day}")
    # logger.debug(f"20 years ago: {date_21y_ago}")

    def __init__(self, symbol: str, debug: bool = False):
        self.extracted_data = None
        self.transformed_data = None
        self.symbol = symbol
        self.debug = debug
        self.transformed_data = None
        logger.setLevel((debug * logging.DEBUG) or logging.INFO)

    def extract(self) -> dict:
        """Hits yFinance API and returns ticker from database."""
        ticker = yf.Ticker(self.symbol)
        self.extracted_data = ticker.info
        return self.extracted_data

    def transform(self) -> dict:
        d = self.extracted_data if self.extracted_data else self.extract()
        self.transformed_data = dict(
            zip=d.get('zip', 'Unknown'),
            sector=d.get('sector', 'Unknown'),
            full_time_employees=d.get('fullTimeEmployees', 'Unknown'),
            long_business_summary=d.get('longBusinessSummary', 'Unknown'),
            city=d.get('city', 'Unknown'),
            phone=d.get('phone', 'Unknown'),
            state=d.get('state', 'Unknown'),
            country=d.get('country', 'Unknown'),
            company_officers=d.get('companyOfficers', 'Unknown'),
            website=d.get('website', 'Unknown'),
            max_age=d.get('maxAge', 'Unknown'),
            address1=d.get('address1', 'Unknown'),
            industry=d.get('industry', 'Unknown'),
            financial_currency=d.get('financialCurrency', 'Unknown'),
            exchange=d.get('exchange', 'Unknown'),
            short_name=d.get('shortName', 'Unknown'),
            long_name=d.get('longName', 'Unknown'),
            exchange_timezone_name=d.get('exchangeTimezoneName', 'Unknown'),
            exchange_timezone_short_name=d.get('exchangeTimezoneShortName', 'Unknown'),
            is_esg_populated=d.get('isEsgPopulated', 'Unknown'),
            gmt_off_set_milliseconds=d.get('gmtOffSetMilliseconds', 'Unknown'),
            quote_type=d.get('quoteType', 'Unknown'),
            symbol=d.get('symbol', 'Unknown'),
            message_board_id=d.get('messageBoardId', 'Unknown'),
            market=d.get('market', 'Unknown'),
            last_split_factor=d.get('lastSplitFactor', 'Unknown'),
            logo_url=d.get('logo_url', 'Unknown'),
            etl_loaded_datetime_utc=dt.datetime.utcnow()
        )
        return self.transformed_data

    def load(self):
        data = self.transformed_data if self.transformed_data else self.transform()
        new_asset = landing.Asset(**data)
        session.add(new_asset)
        session.commit()


if __name__ == '__main__':
    Session = sessionmaker()
    session = Session(bind=engine, expire_on_commit=False)

    for a in ['T', 'AAPL', 'GOOGL']:
        a = AssetPipeline(a)
        a.run_pipeline()
    session.commit()
    session.close()
