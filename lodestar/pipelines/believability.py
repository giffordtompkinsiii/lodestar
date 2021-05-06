import numpy as np 
import pandas as pd

# from .daily import get_daily_scores
from .. import logger
from ..database.models import Asset, PriceHistory, session
from ..database.maps import asset_map, tidemark_map
from ..database.functions import collection_to_dataframe

# Need to calculate new believabilities before importing new prices.
# TODO: Could simply get qtrly_believability, and then join with daily_b score and take weighted average.

b_map = {
        tm.id:np.sum([
            tt.believability_weighting for tt in tm.tidemark_types_collection
        ]) for tm in tidemark_map.values()}

num_func = lambda c: c.map(lambda r: r * b_map[c.name])
den_func = lambda c: c.map(lambda r: pd.notna(r) * b_map[c.name])

def calc_believability(scores:pd.DataFrame) -> pd.DataFrame:
    """Returns numerator, denominator and confidence for scores dataframe."""

    numerator = scores.apply(num_func, axis=0)
    denominator = scores.apply(den_func, axis=0)
    confidence = denominator.applymap(bool)

    return pd.DataFrame({
            'believability': (numerator / denominator).mean(axis=1),
            'confidence': confidence.sum(axis=1) / confidence.count(axis=1)
        })

def get_new_believability(asset: Asset) -> pd.DataFrame:
    logger.info(f"[{asset.id}] {asset.asset} - Collecting Tidemarks.")
    scores_df = collection_to_dataframe(asset.new_prices)[['id']]

    daily_tm_history = [tm for tms in [
                            p.tidemark_history_daily_collection \
                            for p in asset.new_prices
                        ] for tm in tms]

    if daily_tm_history:
        daily_df = collection_to_dataframe(daily_tm_history).score \
                                                            .unstack(
                                                                'tidemark_id')
        scores_df = scores_df.join(daily_df, on='id')

    qtrly_df = collection_to_dataframe(asset.tidemark_history_collection).score \
                                                         .unstack('tidemark_id')



    scores_df = scores_df.join(qtrly_df, how='outer') \
                      .fillna(method='ffill') \
                      .dropna(subset=['id']) \
                      .set_index('id', append=True)

    b_df = calc_believability(scores_df)
    return b_df.reset_index(['asset_id','date'], drop=True)

def get_believability(price: PriceHistory) -> pd.DataFrame:
    logger.info(f"[{price.assets.id}] {price.assets.asset} - Collecting Tidemarks.")
    scores_df = collection_to_dataframe([price])[['id']]

    daily_tm = price.tidemark_history_daily_collection

    if daily_tm:
        daily_df = collection_to_dataframe(daily_tm).score \
                                                    .unstack('tidemark_id')
        scores_df = scores_df.join(daily_df, on='id')

    qtrly_df = collection_to_dataframe(price.tidemark_history_collection).score \
                                                         .unstack('tidemark_id')

    scores_df = scores_df.join(qtrly_df, how='outer') \
                         .fillna(method='ffill') \
                         .dropna(subset=['id']) \
                         .set_index('id', append=True) \
                         .fillna(method='ffill').iloc[-1:,:]

    b = calc_believability(scores_df).reset_index(['asset_id','date'], drop=True)
    price.believability = b.loc[price.id, 'believability']
    price.confidence = b.loc[price.id, 'confidence']
    session.merge(price)
    logger.info(f"[{price.assets.id}] {price.assets.asset} - Updating believability: {price.date}.")
    session.commit()
    session.refresh(price)
    return price


def update_new_believabilities(asset: Asset) -> Asset:
    logger.info(f"[{asset.id}] {asset.asset} - Collecting believability.")
    new_believability = get_new_believability(asset)
    return new_believability
    # for p in asset.new_prices:
    #     p.believability, p.confidence = new_believability.loc[p.id]
    #     session.merge(p)
    #     logger.info(f"[{asset.id}] {asset.asset} - Updating believability: {p.date}")
    #     session.commit()
    # session.refresh(asset)

    # return asset

if __name__=='__main__':
    asset = asset_map[1]
    asset = update_new_believabilities(asset)
