import unittest 
from ...believability.believability import run_believabilities, assets

class TestBelievability(unittest.TestCase):
    def test_believability(self):
        run_believabilities(assets=assets[0:1])

if __name__=='__main__':
    unittest.main()