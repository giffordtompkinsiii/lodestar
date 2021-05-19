import time
from . import logger
from .database.maps import asset_map
from .pipelines.daily import update_old_data


if __name__=='__main__':
    t_0 = time.time()
    for i, asset in enumerate(asset_map.values()):
        update_old_data(asset)
        logger.debug(f"[{i+1}/{len(asset_map)}] {asset.asset} finished: {(time.time()) / 60} mins.")

