"""
Data Pipeline to be run for all assets, daily.

Methods
=======
calc_daily_tidemarks(v, debug=False)->pd.DataFrame:
    Generate the daily tidemark columns.


"""
import time

from sqlalchemy import delete

from .believability import update_new_believabilities

from .. import logger
from ..prices import run_prices
from ..database.maps import asset_map 
from ..database.models import Asset, PriceHistory, session
from ..tidemarks.daily import run_daily_tidemarks

def update_old_data(asset: Asset):
    bad_records = sorted(filter(lambda p: not p.believability,
                             asset.price_history_collection),
                      key=lambda p: p.date)

    if not bad_records:
        return

    bad_record = bad_records[0]
    logger.info(f"{asset.id}-{asset.asset}: Bad price record: {bad_record.date}")
    q = delete(PriceHistory).filter(PriceHistory.asset_id==asset.id) \
                            .filter(PriceHistory.date>=bad_record.date) \
                            .execution_options(synchronize_session="fetch")
    logger.info(f"{asset.id}-{asset.asset}: Deleting {q} records.")
    session.execute(q)
    session.commit()
    return get_new_data(asset)

def get_new_data(asset: Asset):
    if run_prices(asset):
        run_daily_tidemarks(asset)
        logger.debug(
            f"{asset.current_price.date}, {asset.current_price.believability}"
        )
    return asset

if __name__ == '__main__':
    t_0 = time.time()
    for i, asset in enumerate(list(asset_map.values())[0:1]):
        get_new_data(asset)
        logger.debug(f"[{i+1}/{len(asset_map)}] {asset.asset} finished: {(time.time()) / 60} mins.")
