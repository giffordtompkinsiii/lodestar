import unittest 
from ...positions.reports import push_pop_drop_reports, push_trade_report, push_report, BelievabilityReport

class TestPushReports(unittest.TestCase):
    def test_push_believability_report(self):
        push_report(sheet_name='Believability',
                    db_object=BelievabilityReport,
                    columns=['date','asset','believability','confidence'])