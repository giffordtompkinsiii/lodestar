"""
Data AssetPipeline to be run for all assets, daily.

Methods
=======
calc_daily_tidemarks(v, debug=False)->pd.DataFrame:
    Generate the daily tidemark columns.


"""
import time
from sqlalchemy import delete

from . import *
from .buoys import BuoyPipeline
from .prices import PricePipeline

from .tidemarks.daily import run_daily_tidemarks

class EndOfDayPipeline(AssetPipeline):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.price_pipeline = PricePipeline(asset=self.asset)
        self.buoy_pipeline = BuoyPipeline(asset=self.asset, new_asset=False)

    def run_daily_procedures(self):
        self.buoy_pipeline.run_buoys()

    def get_new_data(self):
        pass
        if self.price_pipeline.run_prices(asset):
            run_daily_tidemarks(asset)
            logger.debug(
                f"{asset.current_price.date}, {asset.current_price.believability}"
            )
        return asset


    def update_old_data(self):
        pass
        bad_records = sorted(filter(lambda p: not p.believability,
                                asset.price_history_collection),
                        key=lambda p: p.date)

        if not bad_records:
            return self.get_new_data()

        bad_record = bad_records[0]
        logger.info(f"{asset.id}-{asset.asset}: Bad price record: {bad_record.date}")
        q = delete(PriceHistory).filter(PriceHistory.asset_id==asset.id) \
                                .filter(PriceHistory.date>=bad_record.date) \
                                .execution_options(synchronize_session="fetch")
        logger.info(f"{asset.id}-{asset.asset}: Deleting {q} records.")
        session.execute(q)
        session.commit()
        return self.get_new_data()


if __name__=='__main__':
    t_0 = time.time()
    for a, asset in asset_map.items():
        logger.info(f"{asset.id} - {asset} Running Daily Procedures.")
        d = DailyPipeline(asset)
        d.run_daily_procedures()


