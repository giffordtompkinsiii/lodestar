from lodestar.pipelines.assets import AssetPipeline
from lodestar.database import landing

import datetime as dt


class PricePipeline(AssetPipeline):
    """PricePipeline imports new prices for given asset.
    """
    LandingClass = landing.PriceHistory

    def __init__(self, symbol: str, debug: bool = False):
        super().__init__(symbol, debug)
        self.start_date = '1999-12-31'

    def extract(self) -> dict:
        """Hits yFinance API and returns ticker from database."""
        self.extracted_data = self.ticker.history(start=self.start_date)
        return self.extracted_data

    def transform(self) -> dict:
        data = self.extracted_data.reset_index()
        data.columns = [c.lower().replace(' ', '_') for c in data.columns]
        data['symbol'] = self.symbol
        data['etl_created_utc'] = dt.datetime.utcnow()
        data['etl_updated_utc'] = dt.datetime.utcnow()

        self.transformed_data = data.to_dict(orient='records')
        return self.transformed_data


if __name__ == '__main__':
    for asset in ['AAPL', 'T', 'GOOGL']:
        print(f"Running pipeline for {asset}")
        p = PricePipeline(asset, True)
        p.run_pipeline()
