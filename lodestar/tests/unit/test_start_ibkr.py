from ...database.startup import start_proxy
from ...bod import start_database, start_ibkr

import unittest

# class TestStartDatabase(unittest.TestCase):
#     # start_proxy()

#     def test_start_database(self):
#         self.assertTrue(start_database())

class TestStartIBKR(unittest.TestCase):
    def test_start_ibkr(self):
        self.assertLogs(start_ibkr())

if __name__=='__main__':
    unittest.main()

