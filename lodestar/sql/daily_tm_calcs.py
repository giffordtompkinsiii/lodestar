daily_tm['px_best_peg_ratio'] = (v.bs_sh_out
                                ) / ((v.trail_12m_net_inc_avai_com_share \
                                    * v.eps_growth) or np.nan)

daily_tm['px_pe_ratio'] = (v.bs_sh_out
                        ) / (v.trail_12m_net_inc_avai_com_share or np.nan)

daily_tm['px_px_to_book_ratio'] = (v.bs_sh_out
                                ) / (v.tot_common_eqy or np.nan)

daily_tm['px_current_ev_to_t12m_ebitda_term1'] = (v.bs_sh_out) / (
                                    (v.sales_rev_turn \
                                        - v.trail_12m_cost_of_matl \
                                        - v.is_operating_expn) or np.nan)

daily_tm['px_current_ev_to_t12m_ebitda_term2'] = v.ard_preferred_stock \
                                + v.short_and_long_term_debt \
                                + v.trail_12m_minority_int \
                                - v.cash_and_st_investments
                                ) / ((v.sales_rev_turn \
                                        - v.trail_12m_cost_of_matl \
                                        - v.is_operating_expn) or np.nan)

daily_tm['px_px_to_free_cash_flow'] = 1 / ((v.cf_cash_from_oper \
                                        - v.cf_cap_expend_inc_fix_asset \
                                        - v.net_chng_lt_debt
                                        ) or np.nan)

daily_tm['px_current_ev_to_t12m_fcf'] = 1 * (v.bs_sh_out \
                                    + v.ard_preferred_stock \
                                    + v.short_and_long_term_debt \
                                    + v.trail_12m_minority_int \
                                    - v.cash_and_st_investments
                                ) / ((v.cf_cash_from_oper \
                                        - v.cf_cap_expend_inc_fix_asset \
                                        - v.net_chng_lt_debt) or np.nan)
                                        
daily_tm['px_px_to_sales_ratio'] = 1 / (v.sales_rev_turn or np.nan)