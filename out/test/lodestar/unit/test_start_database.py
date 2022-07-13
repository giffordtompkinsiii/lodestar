from ...database.startup import start_proxy
from ...bod import start_database, start_ibkr

import unittest

class TestStartDatabase(unittest.TestCase):

    def test_start_database(self):
        self.assertTrue(start_database())

if __name__=='__main__':
    unittest.main()