import sqlalchemy
import yfinance
from lodestar.database import landing, engine
from lodestar.pipelines.assets import AssetPipeline
from lodestar.pipelines import Pipeline
from sqlalchemy import PrimaryKeyConstraint, orm

import unittest


class TestAssetPipeline(unittest.TestCase):
    
    def setUp(self):
        self.pipeline = AssetPipeline('T')
        self.session = orm.Session(bind=engine, expire_on_commit=True)
        self.extracted_data = self.pipeline.extract()
        self.maxDiff = 3

    # @unittest.skip
    def test_extract_type(self):
        self.assertIsInstance(self.extracted_data, dict)

    # @unittest.skip
    def test_extract(self):
        self.assertListEqual(list(self.extracted_data.keys()), 
                             list(yfinance.Ticker('T').info.keys()))

    # @unittest.skip
    def test_transform(self):
        transformed_data = self.pipeline.transform(self.extracted_data)
        self.assertTrue(
            set(transformed_data.keys()
            ).issubset(
                set([c.name for c in landing.Asset.__table__.columns]))
        )
    # def test_primary_key(self):
    #     constraint_types = [type(c) for c in self.landing_object.__table__.constraints]
    #     self.assertIn(PrimaryKeyConstraint, constraint_types)

    def tearDown(self):
        self.landing_object = None


if __name__ == '__main__':
    unittest.main()
