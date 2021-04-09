import numpy as np 
import pandas as pd

from ..database.maps import asset_map, tidemark_map
from ..database.functions import collection_to_dataframe

from .prices import get_prices

# Need to calculate new believabilities before importing new prices.
# TODO: Could simply get qtrly_believability, and then join with daily_b score and take weighted average.


def get_qtr_believability(asset):
    # get qtrly believability score.
    b_map = {
        tm.id:np.sum([
            tt.believability_weighting for tt in tm.tidemark_types_collection
        ]) for tm in tidemark_map.values()}

    tm = collection_to_dataframe(asset.tidemark_history_collection)
    tm = tm.unstack('tidemark_id').score

    numer = tm.apply(lambda c: c.map(lambda r: r * b_map[c.name]), 
                     axis=0)
    denom = tm.apply(lambda c: c.map(lambda r: pd.notna(r) * b_map[c.name]), 
                     axis=0)
    conf = denom.applymap(bool)

    return pd.DataFrame({
            'b_qtr': (numer / denom).mean(axis=1),
            'c_qtr': conf.sum(axis=1) / conf.count(axis=1)
        })




if __name__=='__main__':
    asset = asset_map[3]
    prices_df = get_prices(asset)
    df = get_qtr_believability(asset)
    print(df.tail(15))



