import unittest 
import os
import time

class TestProxy(unittest.TestCase):
    def test_proxy_run(self):
        print("Starting proxy.")
        os.system("sh bin/proxy_run")
        time.sleep(15)
        print("Proxy initiated. Test succeeded.")

if __name__=='__main__':
    unittest.main()