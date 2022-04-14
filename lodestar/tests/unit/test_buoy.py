from ...pipelines.buoys import BuoyPipeline, asset_map, Asset
import datetime as dt
from pandas.tseries.offsets import BDay
import unittest
import random

class TestBuoyImport(unittest.TestCase):
    def test_buoy_import(self):
        asset = asset_map[56]
        p = BuoyPipeline(asset, debug=True)
        p.run_buoys()
        self.assertEqual(asset.current_price.date, 
                         dt.date.today() - BDay(1), 
                         "Price History is Out-of-Date")
        self.assertTrue(asset.current_price.buoy_history_collection,
                        "Buoy History is out-of-date.")

if __name__ == '__main__':
    unittest.main()