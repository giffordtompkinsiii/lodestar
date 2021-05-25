"""Submodule containing daily tidemark methods.

Methods
=======
fomat_prices(asset: Asset)->pd.DataFrame
    Format prices for given asset as a `pandas.DataFrame`.

create_daily_tidemarks(v:pd.DataFrame)->pd.DataFrame

"""
from typing import List

from . import *
from ..believability import get_believability
from ...database.models import TidemarkDaily

pd.options.mode.chained_assignment = None  # default='warn'

# TODO: Convert this to go by asset with methods taht work by price_record.
class DailyTidemarkPipeline(TidemarkPipeline):
    daily_cols = {term.id: term for terms in [
            getattr(tm, 'terms_collection', []) for tm in all_query(Tidemark) \
                if tm.daily
        ] for term in terms
    }

    def __init__(self, asset:Asset, debug:bool = False):
        super().__init__(asset=asset, debug=debug)
        self.tm_history = filter(all_query())

    def get_scores(self, dataframe):
        return super().get_scores(dataframe, daily=True)

    def create_daily_tidemarks(self) -> pd.DataFrame:
        """Generate the daily tidemark columns.

        Parameters
        ----------
        price : PriceHistory
            PriceHistory record passed from prices module.
        """
        a = self.asset
        logger.debug(f"Calculating {a.asset} TidemarkDaily - {self.price.date}")
        tidemarks = to_df(self.price.tidemark_history_collection)
        v = tidemarks.reset_index(['date','asset_id']) \
                     .value \
                     .rename(index=self.tm_id_map)
        v['price'] = float(self.price.price)

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
                logger.warning(f"{a.asset} missing {col_name}.")

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
        daily_tm = daily_tm.rename(self.id_tm_map) \
                           .rename_axis('tidemark_id') \
                           .reset_index()
        return daily_tm 

    def get_daily_tidemark_objects(self) -> bool:
        if not self.price.tidemark_history_collection:
            return False
        daily_tm = self.create_daily_tidemarks()
        daily_tm_objs = [TidemarkDaily(price_id=self.price.id, **d._asdict()) \
                            for d in daily_tm.itertuples(index=False) \
                                if not np.isnan(d.value)]
        self.price.tidemark_history_daily_collection = daily_tm_objs
        session.add(self.price)

    def get_tm_history(self):
        a = self.price.assets
        tm_history = session.query(TidemarkDaily) \
                            .filter(TidemarkDaily.price_id.in_(
                                [p.id for p in a.price_history_collection])) \
                            .all()
        return tm_history

    def get_daily_scores(self):
        self.get_daily_tidemark_objects()
        tm_history = self.get_tm_history()

        if tm_history:
            prices = to_df(self.asset.price_history_collection)
            tm_df = to_df(tm_history)
            full_df = prices.join(tm_df.unstack('tidemark_id').value, on='id') \
                            .set_index('id', append=True)
            meds, stds, scores = self.get_scores(full_df)

            for d in self.price.tidemark_history_daily_collection:
                d.med_20y = meds.loc[d.price_id, d.tidemark_id]
                d.std_20y = stds.loc[d.price_id, d.tidemark_id]
                d.score = scores.loc[d.price_id, d.tidemark_id]
                logger.debug(d.__dict__)
                session.merge(d)
            # Calculate the new believability before committing
            self.price = get_believability(self.price)

if __name__=='__main__':
    for asset in asset_map.values():
        prices = sorted(asset.price_history_collection, key=lambda p: p.date)
        for p in prices:
            tm = DailyTidemarkPipeline(p, debug=True)
            df = tm.get_daily_scores()
        logger.info("Committing Believabilities and tidemarks to database.")
        session.commit()
        session.refresh(asset)
