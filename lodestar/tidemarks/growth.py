import numpy as np
import pandas as pd

from ..database.maps import tm_id_name_map, tm_name_id_map
from ..database.models import Asset

def yr_growth(ratio: pd.DataFrame, n: int = 5, freq:str = 'Q'):
    """Calculate Geometric Growth 
    
    Calculate Geometric Growth for given ratio and over the given number `n` of 
    intervals `freq`.
    
    Parameters
    ----------
    ratio : pandas.Series
        Series of ratio values to calculate the growth on.
    n : int
        Number of years of growth. The value will be multiplied by 365 for 
        daily ratios and 4 for quarterly ratios.
    freq: str ('Q' or 'D')
        Denotes 'Quarterly' or 'Daily' intervals to apply n to.

    Returns
    -------
    (tidemark_final_id, tidemark_0_id, growth): (int, int, float)
    """

    freq_params = {
        'periods': n * 4,
        'freq': freq
    }
    if freq == 'D':
        freq_params['periods'] = n * 365

    if ratio.isnull().all():
        pd.Series(np.nan, index=ratio.index)
    ratio_0 = ratio.shift(**freq_params).dropna()

    return (ratio / ratio_0) ** (1/n) - 1


def create_growth_tidemarks(asset, dataframe:pd.DataFrame)->pd.DataFrame:
    """Create growth tidemark columns.
    """

    v = dataframe[['value']].unstack('tidemark_id') \
                 .rename(columns=tm_id_name_map) \
                 .droplevel(0, axis=0) \
                 .droplevel(0, axis=1)

    growth_tm = pd.DataFrame(index=v.index)

    for col_name in ['bs_sh_out',
                        'is_sh_for_diluted_eps',
                        'lt_debt_to_tot_asset',
                        'net_working_capital_investment',
                        'revenue_per_sh',
                        'sales_rev_turn']:
        if col_name not in v.columns:
            v[col_name] = [np.nan] * v.shape[0]
            print(f"{asset.asset} missing {col_name}.")

    growth_tm['eqy_sh_out_5yg'] = yr_growth(v.bs_sh_out, n=5, freq='Q')

    growth_tm['is_sh_for_diluted_eps_5yg'] = yr_growth(v.is_sh_for_diluted_eps,
                                                     n=5, freq='Q')

    growth_tm['lt_debt_to_tot_asset_3yg'] = yr_growth(v.lt_debt_to_tot_asset,
                                                      n=3, freq='Q')

    growth_tm['net_working_capital_investment_5yg'] = yr_growth(
                                            v.net_working_capital_investment, 
                                            n=5, freq='Q')

    growth_tm['revenue_per_sh_5yg'] = yr_growth(v.revenue_per_sh, n=5, freq='Q')

    growth_tm['sales_rev_turn_5yg'] = yr_growth(v.sales_rev_turn, n=5, freq='Q')
    growth_tm['asset_id'] = asset.id
    growth_tm = growth_tm.reset_index() \
                         .set_index(['asset_id','date']) \
                         .sort_index()

    # Ffill method ensures that NA values only exist before history of asset.
    return growth_tm.fillna(method='ffill')

def calculate_growth_tidemarks(asset:Asset, dataframe: pd.DataFrame):
    # dataframe = dataframe.unstack('tidemark_id').rename(columns=tm_id_name_map)
    growth_tm = create_growth_tidemarks(asset, dataframe)

    growth_tm = growth_tm.rename(columns=tm_name_id_map)
    growth_tm.columns.name = 'tidemark_id'

    growth_tm =  growth_tm.stack() \
                         .reorder_levels(['asset_id','tidemark_id','date']) \
                         .sort_index()

    growth_tm.name = 'value'
    return pd.DataFrame(growth_tm)
