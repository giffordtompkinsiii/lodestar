import numpy as np 
import pandas as pd

# from .daily import get_daily_scores

from ..database.maps import asset_map, tidemark_map
from ..database.functions import collection_to_dataframe

# Need to calculate new believabilities before importing new prices.
# TODO: Could simply get qtrly_believability, and then join with daily_b score and take weighted average.

b_map = {
        tm.id:np.sum([
            tt.believability_weighting for tt in tm.tidemark_types_collection
        ]) for tm in tidemark_map.values()}

def calc_believability(scores:pd.DataFrame, daily=False):
    """Returns numerator, denominator and confidence for scores dataframe."""
    num_func = lambda c: c.map(lambda r: r * b_map[c.name])
    den_func = lambda c: c.map(lambda r: pd.notna(r) * b_map[c.name])

    numerator = scores.apply(num_func, axis=0)
    denominator = scores.apply(den_func, axis=0)
    confidence = denominator.applymap(bool)

    suffix = (daily * '_day') or '_qtr'

    return pd.DataFrame({
            'b'   + suffix: (numerator / denominator).mean(axis=1),
            'sum' + suffix: confidence.sum(axis=1),
            'cnt' + suffix: confidence.count(axis=1)
        })

def get_qtrly_scores(asset):
    pass


    

if __name__=='__main__':
    asset = asset_map[3]
    # qtr_df = get_believability(asset)
    # print(qtr_df.tail(15))



