"""Submodule containing daily tidemark methods.

Methods
=======
fomat_prices(asset: Asset)->pd.DataFrame
    Format prices for given asset as a `pandas.DataFrame`.

create_daily_tidemarks(v:pd.DataFrame)->pd.DataFrame

"""
from typing import List

from . import *
# , tm_id_map, id_tm_map
from ..database.models import Asset, PriceHistory, TidemarkDaily, session
from ..database.functions import add_new_objects, collection_to_dataframe
from ..pipelines.believability import get_believability

pd.options.mode.chained_assignment = None  # default='warn'

daily_cols = {
        term.id: term for terms in [
            getattr(tm, 'terms_collection', []) for tm in all_query(Tidemark) \
                if tm.daily
        ] for term in terms
    }


def create_daily_tidemarks(price: PriceHistory) -> pd.DataFrame:
    """Generate the daily tidemark columns.

    Parameters
    ----------
    price : PriceHistory
        PriceHistory record passed from prices module.
    """
    logger.info(f"Calculating {price.assets.asset} TidemarkDaily - {price.date}")
    tidemarks = to_df(price.tidemark_history_collection)
    v = tidemarks.reset_index(['date','asset_id']) \
                    .value \
                    .rename(index=tm_id_map)
    v['price'] = float(price.price)

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
        if col_name not in v.index:
            v[col_name] = np.nan
            logger.warning(f"{price.assets.asset} missing {col_name}.")

    daily_tm = pd.Series(dtype='float', name='value')

    daily_tm['best_peg_ratio'] = (v.price * v.bs_sh_out
                                    ) / ((v.trail_12m_net_inc_avai_com_share \
                                          * v.eps_growth) or np.nan)
    daily_tm['pe_ratio'] = (v.price * v.bs_sh_out
                            ) / (v.trail_12m_net_inc_avai_com_share or np.nan)
    daily_tm['px_to_book_ratio'] = (v.price * v.bs_sh_out
                                    ) / (v.tot_common_eqy or np.nan)
    daily_tm['current_ev_to_t12m_ebitda'] = (
                        v.price * v.bs_sh_out \
                            + v.ard_preferred_stock \
                            + v.short_and_long_term_debt \
                            + v.trail_12m_minority_int \
                            - v.cash_and_st_investments
                            ) / ((v.sales_rev_turn \
                                    - v.trail_12m_cost_of_matl \
                                    - v.is_operating_expn) or np.nan)
    daily_tm['px_to_free_cash_flow'] = (v.price
                                            ) / ((v.cf_cash_from_oper \
                                                - v.cf_cap_expend_inc_fix_asset \
                                                - v.net_chng_lt_debt
                                                ) or np.nan)
    daily_tm['current_ev_to_t12m_fcf'] = v.price * (v.bs_sh_out \
                                            + v.ard_preferred_stock \
                                            + v.short_and_long_term_debt \
                                            + v.trail_12m_minority_int \
                                            - v.cash_and_st_investments
                                        ) / ((v.cf_cash_from_oper \
                                                - v.cf_cap_expend_inc_fix_asset \
                                                - v.net_chng_lt_debt) or np.nan)
    daily_tm['px_to_sales_ratio'] = v.price / (v.sales_rev_turn or np.nan)
    daily_tm = daily_tm.rename(id_tm_map) \
                       .rename_axis('tidemark_id') \
                       .reset_index()

    return daily_tm 

def get_daily_tidemark_objects(price: PriceHistory) -> List[TidemarkDaily]:
    daily_tm = create_daily_tidemarks(price)
    daily_tm_objs = [TidemarkDaily(price_id=price.id, **d._asdict()) \
                        for d in daily_tm.itertuples(index=False) \
                            if not np.isnan(d.value)]
    price.tidemark_history_daily_collection = daily_tm_objs
    return price 

def get_daily_scores(price: PriceHistory):
    tm_history = get_tm_history(price)

    price = get_daily_tidemark_objects(price)

    if not price.tidemark_history_daily_collection:
        return []
    prices = collection_to_dataframe([price])
    tm_df = collection_to_dataframe(
                        [*tm_history, *price.tidemark_history_daily_collection])
    full_df = prices.join(tm_df.unstack('tidemark_id').value, on='id') \
                    .set_index('id', append=True)

    meds, stds, scores = get_scores(full_df, daily=True)

    for d in price.tidemark_history_daily_collection:
        d.med_20y = meds.loc[d.price_id, d.tidemark_id]
        d.std_20y = stds.loc[d.price_id, d.tidemark_id]
        d.score = scores.loc[d.price_id, d.tidemark_id]

    session.add_all(price.tidemark_history_daily_collection)
    session.commit()
    session.refresh(price)
    price = get_believability(price)
    session.refresh(price.assets)

def get_tm_history(price):
    tm_history = session.query(TidemarkDaily) \
                        .filter(TidemarkDaily.price_id.in_(
                            [p.id for p in price.assets.price_history_collection])
                        ).all()
    return tm_history


def run_daily_tidemark(price: PriceHistory):
    """Insert new daily tidemarks and return records.

    Refreshes `price` record after being passed.

    Arguements
    ==========
    PriceHistory
        Record from price_history used to calculate new TidemarkDaily records.
    
    Returns
    =======
    list(TidemarkDaily)
        Records for the tidemark_history_daily table for the given PriceHistory 
        record.
    
    """
    daily_tidemark_objects = get_daily_tidemark_objects(price)
    daily_tidemark_objects = add_new_objects(daily_tidemark_objects, 
                                             refresh_object=price)
    return daily_tidemark_objects


def run_daily_tidemarks(asset: Asset) -> List[TidemarkDaily]:
    """Create and export new TidemarkDaily records to database."""
    for price in asset.new_prices:
        get_daily_scores(price)


if __name__=='__main__':
    asset = asset_map[5]
    new_tidemarks = run_daily_tidemarks(asset)
