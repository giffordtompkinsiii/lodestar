-- SELECT ',(p.'||column_name||', '''||column_name||''')'
-- FROM information_schema.columns 
-- WHERE table_name = 'tidemark_history' 
--     AND table_schema = 'landing';
DROP TABLE IF EXISTS ods.tidemark_score_history;
WITH base AS (
    SELECT a.id AS asset_id 
        ,d.date_key
        ,h.* 
    FROM landing.tidemark_history h
        INNER JOIN ods.assets a ON a.symbol = SPLIT_PART(h.security, ' ', 1) AND a.is_active
        INNER JOIN presentation.dim_date d ON d.full_date = h.date
),unpivot AS (
    select p.asset_id, p.date_key, t.tidemark_name, t.tidemark_value
    from base p
    cross join lateral (
        values 
    (p.tang_book_val_per_sh, 'tang_book_val_per_sh')
    ,(p.free_cash_flow_per_sh, 'free_cash_flow_per_sh')
    ,(p.fcf_per_dil_shr, 'fcf_per_dil_shr')
    ,(p.tot_debt_to_tot_eqy, 'tot_debt_to_tot_eqy')
    ,(p.quick_ratio, 'quick_ratio')
    ,(p.dvd_payout_ratio, 'dvd_payout_ratio')
    ,(p.gross_margin, 'gross_margin')
    ,(p.sales_rev_turn, 'sales_rev_turn')
    ,(p.cf_cash_from_oper, 'cf_cash_from_oper')
    ,(p.cur_ratio, 'cur_ratio')
    ,(p.ebitda_to_revenue, 'ebitda_to_revenue')
    ,(p.net_working_capital_investment, 'net_working_capital_investment')
    ,(p.trail_12m_free_cash_flow, 'trail_12m_free_cash_flow')
    ,(p.return_com_eqy, 'return_com_eqy')
    ,(p.return_on_work_cap, 'return_on_work_cap')
    ,(p.return_on_inv_capital, 'return_on_inv_capital')
    ,(p.invent_turn, 'invent_turn')
    ,(p.acct_rcv_turn, 'acct_rcv_turn')
    ,(p.asset_turnover, 'asset_turnover')
    ,(p.return_on_asset, 'return_on_asset')
    ,(p.fcf_to_total_debt, 'fcf_to_total_debt')
    ,(p.geo_grow_cur_ratio, 'geo_grow_cur_ratio')
    ,(p.geo_grow_cash_oper_act, 'geo_grow_cash_oper_act')
    ,(p.geo_grow_roa, 'geo_grow_roa')
    ,(p.fcf_to_firm_5_year_growth, 'fcf_to_firm_5_year_growth')
    ,(p.revenue_per_sh, 'revenue_per_sh')
    ,(p.is_sh_for_diluted_eps, 'is_sh_for_diluted_eps')
    ,(p.lt_debt_to_tot_asset, 'lt_debt_to_tot_asset')
    ,(p.bs_sh_out, 'bs_sh_out')
    ,(p.short_and_long_term_debt, 'short_and_long_term_debt')
    ,(p.cash_and_st_investments, 'cash_and_st_investments')
    ,(p.tot_common_eqy, 'tot_common_eqy')
    ,(p.is_operating_expn, 'is_operating_expn')
    ,(p.ard_preferred_stock, 'ard_preferred_stock')
    ,(p.trail_12m_net_inc_avai_com_share, 'trail_12m_net_inc_avai_com_share')
    ,(p.trail_12m_minority_int, 'trail_12m_minority_int')
    ,(p.eps_growth, 'eps_growth')
    ,(p.net_chng_lt_debt, 'net_chng_lt_debt')
    ,(p.geo_grow_gross_margin, 'geo_grow_gross_margin')
    ,(p.geo_grow_inv_turn, 'geo_grow_inv_turn')
    ,(p.geo_grow_net_rev_margin, 'geo_grow_net_rev_margin')
    ,(p.trail_12m_cost_of_matl, 'trail_12m_cost_of_matl')
    ,(p.cf_cap_expend_inc_fix_asset, 'cf_cap_expend_inc_fix_asset')
    ) as t(tidemark_value, tidemark_name)
), score_terms AS (
    SELECT u.asset_id
        ,u.date_key
        -- ,tm.id AS tidemark_id
        ,u.tidemark_name AS tidemark
        ,u.tidemark_value AS tidemark_value
        ,MEDIAN(u.tidemark_value) OVER w AS tidemark_median
        ,STDDEV_SAMP(u.tidemark_value) OVER w AS tideamrk_std
    FROM unpivot u
    -- INNER JOIN landing.tidemarks tm ON tm.tidemark = u.tidemark_name
    WINDOW w AS (PARTITION BY asset_id, tidemark_name /*tm.id*/ ORDER BY date_key ROWS BETWEEN 80 PRECEDING AND CURRENT ROW)
), final AS (
    SELECT *
        ,0.5 + (s.tidemark_value - s.tidemark_median) / NULLIF((2 * 1.382 * s.tideamrk_std), 0) tidemark_score
        ,CURRENT_TIMESTAMP AS etl_created_datetime
        ,CURRENT_TIMESTAMP AS etl_updated_datetime
    FROM score_terms s
)
SELECT * 
INTO ods.tidemark_score_history
FROM final s 
ORDER BY s.tidemark, s.date_key DESC, s.asset_id;
SELECT * FROM ods.tidemark_score_history;