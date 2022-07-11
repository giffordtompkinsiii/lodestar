from pipelines.bulk_pipelines.prices import PricePipeline, asset_map
import datetime as dt
from pandas.tseries.offsets import BDay
import unittest
import random

random.seed(a=42)

class TestPriceImport(unittest.TestCase):
    def test_price_import(self):
        
        asset = list(asset_map.values())[0]
        p = PricePipeline(asset, debug=True)
        self.assertEqual(p.run_prices().date, 
                         dt.date.today() - BDay(1))

if __name__ == '__main__':
    unittest.main()