from lodestar.pipelines import AssetPipeline
from lodestar.database.models import Asset, session
from lodestar.database.maps import asset_map
from lodestar.pipelines.prices import PricePipeline

import yfinance as yf

a_map = {a.asset: a for a in asset_map.values()}

new_assets_str = input("Type in new asset names separated by a comma:")

new_asset_names = new_assets_str.upper().replace(' ','').split(',')

for asset_name in new_asset_names:
    if asset_name in a_map.keys():
        print(f"Asset '{asset_name}' already in database.")
    else:
        a = yf.Ticker(asset_name)
        if a.info.get('regularMarketPrice'):
            a = Asset()
            a.asset = asset_name
            session.add(a)
            session.commit()
            session.refresh(a)
            p = PricePipeline(asset=a, debug=False)
            p.run_prices()
        else:
            print(f"No price history data for '{asset_name}' in YahooFianance.")

            

