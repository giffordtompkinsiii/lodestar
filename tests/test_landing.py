from lodestar.database.landing import Base, LandingAsset, LandingPriceHistory, LandingTidemarkHistory

from sqlalchemy.sql.schema import PrimaryKeyConstraint
from sqlalchemy.orm import sessionmaker

import unittest

class TestLandingTables(unittest.TestCase):
    
    def setUp(self):
        self.landing_object = LandingAsset()

    def test_schema(self):
        self.assertEqual('landing', self.landing_object.metadata.schema)
    
    def test_primary_key(self):
        constraint_types = [type(c) for c in self.landing_object.__table__.constraints]
        self.assertIn(PrimaryKeyConstraint, constraint_types)

    def tearDown(self):
        self.landing_object = None


if __name__=='__main__':
    unittest.main()
