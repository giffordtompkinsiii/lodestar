from lodestar.database import landing, engine
from lodestar.pipelines import Pipeline

from sqlalchemy.orm.session import sessionmaker

import datetime as dt
import yfinance as yf


class AssetPipeline(Pipeline):
    LandingClass = landing.Asset

    def __init__(self, symbol: str, debug: bool = False):
        super().__init__(symbol, debug)
        self.extracted_data = None
        self.transformed_data = None
        self.ticker = yf.Ticker(self.symbol)

    def extract(self) -> dict:
        """Hits yFinance API and returns ticker from database."""
        self.extracted_data = self.ticker.info
        return self.extracted_data

    def transform(self):
        d = self.extracted_data if self.extracted_data else self.extract()
        self.transformed_data = [dict(
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
            etl_created_utc=dt.datetime.utcnow(),
            etl_updated_utc=dt.datetime.utcnow()
        )]
        return self.transformed_data


if __name__ == '__main__':
    Session = sessionmaker()
    session = Session(bind=engine, expire_on_commit=False)

    for a in ['T', 'AAPL', 'GOOGL']:
        a = AssetPipeline(a)
        a.run_pipeline()
    session.commit()
    session.close()
