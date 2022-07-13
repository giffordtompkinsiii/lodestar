from lodestar.apis.bloomberg import BloombergAPI
from lodestar.database import landing
from lodestar.pipelines import Pipeline
import datetime as dt


class TidemarkPipeline(Pipeline, BloombergAPI):
    LandingClass = landing.TidemarkHistory

    def __init__(self, symbol, debug):
        super().__init__(symbol, debug)

    def extract(self):
        self.extracted_data = self.get_tidemarks(self.symbol)
        return self.extracted_data

    def transform(self):
        data = self.extracted_data if self.extracted_data else self.extract()
        data['etl_created_utc'] = dt.datetime.utcnow()
        data['etl_updated_utc'] = dt.datetime.utcnow()
        self.transformed_data = data.reset_index().to_dict(orient='records')

        return self.transformed_data


if __name__ == '__main__':
    for asset in ['AAPL', 'T', 'GOOGL']:
        print(f"Running pipeline for {asset}")
        p = TidemarkPipeline(asset, True)
        p.run_pipeline()
