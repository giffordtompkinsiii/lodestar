from ..tidemarks import get_scores

from ..database.maps import asset_map, tidemark_map, tm_name_id_map
from ..database.models import Asset, PriceHistory
from ..database.functions import (collection_to_dataframe as to_df,
                                  update_database_object)

from .prices import get_prices
from .believability import calc_believability
from .import_procedures import get_tidemark_scores

import datetime as dt
import pandas as pd
import numpy as np

asset_id = 1
asset = asset_map[asset_id]

tm_name_map = {tm_id:tm.tidemark for tm_id,tm in tidemark_map.items()}


def calc_daily_tidemarks(v, debug=False)->pd.DataFrame:
    """Generate the daily tidemark columns.

    Parameters
    ----------
    v : pd.DataFrame
        tidemarks dataframe joined with prices
    """
    daily_tm = pd.DataFrame(index=v.index)

    for col_name in ['ard_preferred_stock', 
                        'bs_sh_out',
                        'cash_and_st_investments',
                        'cf_cap_expend_inc_fix_asset',
                        'cf_cash_from_oper',
                        'eps_growth',
                        'is_operating_expn',
                        'net_chng_lt_debt',
                        'sales_rev_turn',
                        'short_and_long_term_debt',
                        'tot_common_eqy',
                        'trail_12m_cost_of_matl',
                        'trail_12m_minority_int',
                        'trail_12m_net_inc_avai_com_share']:
        if col_name not in v.columns:
            v[col_name] = np.nan
            if debug:
                print(f"Asset missing {col_name}.")

    daily_tm['best_peg_ratio'] = (v.price * v.bs_sh_out
                                    ) / (v.trail_12m_net_inc_avai_com_share \
                                        * v.eps_growth).replace(0, np.nan)
    daily_tm['pe_ratio'] = (v.price * v.bs_sh_out
                            ) / (
                                v.trail_12m_net_inc_avai_com_share\
                                 .replace(0, np.nan))
    daily_tm['px_to_book_ratio'] = (v.price * v.bs_sh_out
                                    ) / (v.tot_common_eqy.replace(0, np.nan))
    daily_tm['current_ev_to_t12m_ebitda'] = (
                        v.price * v.bs_sh_out \
                            + v.ard_preferred_stock \
                            + v.short_and_long_term_debt \
                            + v.trail_12m_minority_int \
                            - v.cash_and_st_investments
                            ) / (
                                v.sales_rev_turn \
                                    - v.trail_12m_cost_of_matl \
                                    - v.is_operating_expn).replace(0, np.nan)
    daily_tm['px_to_free_cash_flow'] = (v.price
                                            ) / (v.cf_cash_from_oper \
                                                - v.cf_cap_expend_inc_fix_asset \
                                                - v.net_chng_lt_debt
                                                ).replace(0, np.nan)
    daily_tm['current_ev_to_t12m_fcf'] = v.price * (v.bs_sh_out \
                                            + v.ard_preferred_stock \
                                            + v.short_and_long_term_debt \
                                            + v.trail_12m_minority_int \
                                            - v.cash_and_st_investments
                                        ) / (v.cf_cash_from_oper \
                                                - v.cf_cap_expend_inc_fix_asset \
                                                - v.net_chng_lt_debt
                                            ).replace(0, np.nan)
    daily_tm['px_to_sales_ratio'] = v.price / (
                                        v.sales_rev_turn).replace(0, np.nan)
    return daily_tm

def get_historical_prices(asset) -> pd.DataFrame:
    historical_price_df = to_df(asset.price_history_collection) \
                                     .reorder_levels(['asset_id','date'], axis=0)

    last_date = asset.current_price.date
    new_prices = get_prices(asset, last_date=last_date)

    return historical_price_df.combine_first(new_prices)

def get_daily_tidemarks(asset: Asset):
    price_df = get_historical_prices(asset)
    # pull the relevant qtrly tidemarks.
    tm_df = to_df(asset.tidemark_history_collection) \
                       .reorder_levels(['asset_id','date','tidemark_id'])

    tm_df = tm_df.value.unstack('tidemark_id')

    v = price_df.join(tm_df, how='outer').rename(columns=tm_name_map) 

    daily_tm = v.fillna(method='ffill')
    dataframe = calc_daily_tidemarks(daily_tm).dropna(how='all')

    return dataframe

def get_daily_scores(asset):
    daily_tm = get_daily_tidemarks(asset)
    tidemarks = daily_tm.rename(columns=tm_name_id_map)
    tidemarks.columns.name = 'tidemark_id'
    tidemarks = pd.DataFrame(tidemarks.stack() \
                                      .reorder_levels(
                                          ['asset_id','tidemark_id','date']) \
                                      .sort_index(), 
                             columns=['value'])

    return get_tidemark_scores(tidemarks, daily=True)

# TODO: add option to just do both daily and quarterly. This is messy.
def get_believability(asset, daily=None):
    if daily is None:
        scores_day = get_daily_scores(asset).unstack('tidemark_id').score
        day_b = calc_believability(scores_day, daily=True)

        tidemarks = to_df(asset.tidemark_history_collection)
        scores_qtr = tidemarks.unstack('tidemark_id').score
        qtr_b = calc_believability(scores_qtr, daily=False)
        return day_b.join(qtr_b).sort_index().fillna(method='ffill')
    elif daily:
        scores_day = get_daily_scores(asset).unstack('tidemark_id').score
        return calc_believability(scores_day, daily=daily)
    else: 
        tidemarks = to_df(asset.tidemark_history_collection)
        scores_qtr = tidemarks.unstack('tidemark_id').score
        return calc_believability(scores_qtr, daily=daily)

if __name__ == '__main__':
    asset = asset_map[3]
    prices_df = get_historical_prices(asset)
    daily_tm = get_daily_tidemarks(asset)
    scores_df = get_daily_scores(asset)
    b = get_believability(asset, daily=None)
    b['believability'] = ((b.b_day * b.sum_day) + (b.b_qtr + b.sum_qtr)) \
                            / (b.cnt_day + b.cnt_qtr)
    b['confidence'] = (b.sum_day + b.sum_qtr) / (b.cnt_day + b.cnt_qtr)
    import_df = prices_df.combine_first(b[['believability','confidence']])
    # asset = update_database_object(import_df=import_df)
    
