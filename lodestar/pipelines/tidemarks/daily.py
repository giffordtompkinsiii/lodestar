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
from ...database.models import TidemarkHistory #TidemarkDaily, 
from ...database.landing import TidemarkDaily
from ..prices import PricePipeline

pd.options.mode.chained_assignment = None  # default='warn'

class DailyTidemarkByAsset(PricePipeline, TidemarkPipeline):
    daily_cols = {term.id: term for terms in [
            getattr(tm, 'terms_collection', []) for tm in all_query(Tidemark) \
                if tm.daily
        ] for term in terms
    }
    def __init__(self, asset:Asset, debug:bool = False):
        super(DailyTidemarkByAsset, self).__init__(asset=asset, debug=debug)

# TODO: Convert this to go by asset with methods that work by price_record.
class DailyTidemarkPipeline(TidemarkPipeline):
    daily_cols = {term.id: term for terms in [
            getattr(tm, 'terms_collection', []) for tm in all_query(Tidemark) \
                if tm.daily
        ] for term in terms
    }
    # unique_key = 

    def __init__(self, asset:Asset, debug:bool = False):
        super().__init__(asset=asset, debug=debug)

    def get_scores(self, dataframe):
        return super().get_scores(dataframe, daily=True)

    def create_tidemark_cols(self, v):
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
                logger.debug(f"{self.asset.asset} missing {col_name}.")

        daily_tm = pd.Series(dtype='float', name='value')

        daily_tm['best_peg_ratio'] = (v.price * v.bs_sh_out
                                        ) / ((v.trail_12m_net_inc_avai_com_share \
                                            * v.eps_growth).replace(0, np.nan))
        daily_tm['pe_ratio'] = (v.price * v.bs_sh_out
                                ) / (v.trail_12m_net_inc_avai_com_share.replace(0, np.nan))
        daily_tm['px_to_book_ratio'] = (v.price * v.bs_sh_out
                                        ) / (v.tot_common_eqy.replace(0, np.nan))
        daily_tm['current_ev_to_t12m_ebitda'] = (
                                    v.price * v.bs_sh_out \
                                        + v.ard_preferred_stock \
                                        + v.short_and_long_term_debt \
                                        + v.trail_12m_minority_int \
                                        - v.cash_and_st_investments
                                        ) / ((v.sales_rev_turn \
                                                - v.trail_12m_cost_of_matl \
                                                - v.is_operating_expn).replace(0, np.nan))
        daily_tm['px_to_free_cash_flow'] = (v.price
                                            ) / ((v.cf_cash_from_oper \
                                                - v.cf_cap_expend_inc_fix_asset \
                                                - v.net_chng_lt_debt
                                                ).replace(0, np.nan))
        daily_tm['current_ev_to_t12m_fcf'] = v.price * (v.bs_sh_out \
                                            + v.ard_preferred_stock \
                                            + v.short_and_long_term_debt \
                                            + v.trail_12m_minority_int \
                                            - v.cash_and_st_investments
                                        ) / ((v.cf_cash_from_oper \
                                                - v.cf_cap_expend_inc_fix_asset \
                                                - v.net_chng_lt_debt).replace(0, np.nan))
        daily_tm['px_to_sales_ratio'] = v.price / (v.sales_rev_turn.replace(0, np.nan))
        daily_tm['price_id'] = v.id
        daily_tm = daily_tm.rename(self.id_tm_map, errors='ignore') \
                           .rename_axis('tidemark_id') \
                           .reset_index()
        return daily_tm 

    def create_daily_tidemarks_by_asset(self) -> pd.DataFrame:
        """Generate the daily tidemark columns.

        Parameters
        ----------
        price : PriceHistory
            PriceHistory record passed from prices module.
        """
        a = self.asset
        logger.debug(f"Calculating {a.asset} TidemarkDaily")
        tidemarks = to_df(a.tidemark_history_collection).value
        prices = to_df(a.price_history_collection)
        v = prices.join(tidemarks.unstack()) \
                  .fillna(method='ffill') \
                  .rename(columns=self.tm_id_map)
        return self.create_tidemark_cols(v)


    def create_daily_tidemarks(self, price:PriceHistory) -> pd.DataFrame:
        """Generate the daily tidemark columns.

        Parameters
        ----------
        price : PriceHistory
            PriceHistory record passed from prices module.
        """
        a = self.asset
        logger.debug(f"Calculating {a.asset} TidemarkDaily - {price.date}")
        tidemarks = to_df(price.tidemark_history_collection)
        v = tidemarks.reset_index(['date','asset_id']) \
                     .value \
                     .rename(index=self.tm_id_map)
        v['price'] = float(price.price)
        v['id'] = price.id
        return v
        # return self.create_tidemark_cols(v)

    def _explode_daily_tidemark(self, s):
        s.name = 'value'
        return s.reset_index()

    def get_daily_tidemark_objects_by_asset(self) -> bool:
        daily_tm = self.create_daily_tidemarks_by_asset()
        daily_tm_objs = [[TidemarkDaily(tidemark_id=d.tidemark_id, **i._asdict()) \
                            for i in self._explode_daily_tidemark(d.value).itertuples(index=False) if not np.isnan(i.value)
                            ] for d in daily_tm.itertuples(index=False)]
        
        # )
        # (**d._asdict()) for tm in ]\
        #                     for d in daily_tm.itertuples(index=False) \
        #                         if not np.isnan(d.value)]
        session.add_all(daily_tm_objs)
        return daily_tm_objs

    def get_daily_tidemark_objects(self, price: PriceHistory) -> bool:
        daily_tm = self.create_daily_tidemarks(price)
        daily_tm_objs = [TidemarkDaily(price_id=price.id, **d._asdict()) \
                            for d in daily_tm.itertuples(index=False) \
                                if not np.isnan(d.value)]
        price.tidemark_history_daily_collection = daily_tm_objs
        session.merge(price)
        return price

    def get_tm_history(self):
        a = self.asset
        self.tm_history = session.query(TidemarkDaily) \
                            .filter(TidemarkDaily.price_id.in_(
                                [p.id for p in a.price_history_collection])) \
                            .all()
        return self.tm_history

    def get_daily_scores_by_asset(self):
        self.get_daily_tidemark_objects_by_asset()
        tm_history = self.get_tm_history()

    def get_daily_scores(self, by_asset: bool = False):
        updatable_prices = sorted(
                            filter(lambda p: not bool(p.believability),
                                self.asset.price_history_collection),
                            key = lambda p: p.date)
        if by_asset:
            self.get_daily_tidemark_objects_by_asset()
        else:
            for p in updatable_prices:
                if not p.tidemark_history_collection:
                    logger.debug(f"Price already has believability. If you wish " 
                            + "to recalculate, specify `audit` parameter.")
                    continue
                updatable_prices.append(self.get_daily_tidemark_objects(p))
        tm_history = self.get_tm_history()
        if tm_history:
            prices = to_df(self.asset.price_history_collection)
            tm_df = to_df(tm_history)
            full_df = prices.join(tm_df.unstack('tidemark_id').value, on='id') \
                            .set_index('id', append=True)
            meds, stds, scores = self.get_scores(full_df)
            for price in updatable_prices:
                for d in price.tidemark_history_daily_collection:
                    d.med_20y = meds.loc[d.price_id, d.tidemark_id]
                    d.std_20y = stds.loc[d.price_id, d.tidemark_id]
                    d.score = scores.loc[d.price_id, d.tidemark_id]
                    logger.debug(d.__dict__)
                    session.merge(d)
                # Calculate the new believability before committing
                price = get_believability(price)

if __name__=='__main__':
    for asset in asset_map.values():
        d = DailyTidemarkPipeline(asset, debug=True)
        df = d.create_daily_tidemarks_by_asset()
        # TODO: This creates a dataframe with 7 rows (one for each daily_tidemark applicable). 
        # The values are nested series with asset_id and date. I need to get `price_id` and `tidemark_id` to the index.
        daily_tms = {v._asdict()['tidemark_id']:v for v in df.itertuples()}
        prices = pd.DataFrame({'price_id':daily_tms.pop('price_id').value})
        tidemarks = pd.DataFrame({k: v.value for k,v in daily_tms.items()})

        meds = tidemarks.rolling(window=252 * 20, 
                          min_periods=252).median()

        stds = tidemarks.rolling(window=252 * 20, 
                          min_periods=252).std()

        scores = (0.5 + (tidemarks - meds) / (2 * 1.382 * stds))
        final_df = pd.DataFrame({'value':tidemarks.stack(), 'med_20y':meds.stack(), 'std_20y':stds.stack(), 'score':scores.stack()}).unstack()
        final_df.index = final_df.index.map(prices.price_id)
        final_df = final_df.stack(level=1)
        final_df.index.names = ['price_id', 'tidemark_id']
        daily_tm_objects = [TidemarkDaily(**d._asdict()) for d in final_df.reset_index().itertuples(index=False)]
        session.merge(daily_tm_objects)
        break

        # d.get_daily_scores(by_asset=True)
        # logger.info("Committing Believabilities and tidemarks to database.")
        # session.commit()
        # session.refresh(asset)
