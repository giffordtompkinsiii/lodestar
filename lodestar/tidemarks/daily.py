"""Submodule containing daily tidemark methods.

Methods
=======
fomat_prices(asset: Asset)->pd.DataFrame
    Format prices for given asset as a `pandas.DataFrame`.

create_daily_tidemarks(v:pd.DataFrame)->pd.DataFrame

"""
from . import *

pd.options.mode.chained_assignment = None  # default='warn'

daily_cols = {
        term.id: term for terms in [
            getattr(tm, 'terms_collection', []) for tm in all_query(Tidemark) \
                if tm.daily
        ] for term in terms
    }

def format_prices(asset: Asset):
    '''Format prices for given asset.
    
    Return the historical prices dataframe in a format ready for joining to the 
    asset's tidemarks.
    '''
    return to_df(query_results=asset.price_history_collection,
                 db_table=PriceHistory).rename(columns={'id':'price_id'}) # .drop(columns=['id'])

def get_asset_terms(asset: Asset)->pd.DataFrame:
    try:
        tidemarks_day = [tm for tm in asset \
                                        # .current_price \
                                        .tidemark_history_collection \
                            if tm.tidemark_id in daily_cols]
    except:
        print(asset.asset, "does not have current tidemark_history_collection")
        return pd.DataFrame()
    day_df = format_tidemarks(asset=asset, tidemarks_collection=tidemarks_day)
    if day_df.empty:
        return pd.DataFrame()
    prices_df = format_prices(asset)
    terms_df = prices_df.join(day_df, how='left').fillna(method='ffill')
    return terms_df


def create_daily_tidemarks(asset: Asset)->pd.DataFrame:
    """Generate the daily tidemark columns.

    Parameters
    ----------
    v : pd.DataFrame
        tidemarks dataframe joined with prices
    """
    v = get_asset_terms(asset)
    if v.empty:
        return pd.DataFrame()
    daily_tm = v[['price_id']]

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
    return daily_tm.set_index('price_id', append=True)


def get_asset_daily_scores(asset: Asset):
    """Returns the given asset's daily tidemark scores.

    Function formats daily prices and appropriate tidemarks to calculate the 
    assets daily tidemarks' individual medians, standard deviations and resulting 
    scores.

    Returns
    =======
    (meds_daily, stds_daily, scores_daily) : tuple(pd.DataFrames)
        The historical rolling median, standard deviations and then the 
        percentile the current value falls within the scoring algorithm.
    """
    # Extract qauarterly tidemarks needed to calculate daily tidemarks 

    daily_df = create_daily_tidemarks(asset)
    ## This is where we have to join with the past tidemarks.
    # print("daily_df shape", daily_df.shape)
    meds_daily, stds_daily, scores_daily = get_scores(daily_df, freq_per_yr=252)

    return meds_daily, stds_daily, scores_daily



if __name__=='__main__':
    asset = asset_map[626]
    meds_daily, stds_daily, scores_daily = get_asset_daily_scores(asset)
    print(scores_daily)


