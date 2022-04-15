SET search_path = 'financial';
DROP VIEW IF EXISTS vw_bloomberg_daily_calculated;
CREATE VIEW vw_bloomberg_daily_calculated AS (
WITH t AS (
    SELECT th.asset_id, a.asset, tm.tidemark, th.date, th.tidemark_value 
    FROM tidemarks tm 
        LEFT JOIN bloomberg_tidemarks th ON th.tidemark_id = tm.id
        INNER JOIN assets a ON a.id = th.asset_id
), tidemark_pivot AS (
    SELECT DISTINCT ON (t.asset, t.date) t.asset_id
        ,t.asset
        ,t.date 
        ,bs_sh_out.value bs_sh_out
        ,trail_12m_net_inc_avai_com_share.value trail_12m_net_inc_avai_com_share
        ,eps_growth.value eps_growth
        ,tot_common_eqy.value tot_common_eqy
        ,sales_rev_turn.value sales_rev_turn
        ,trail_12m_cost_of_matl.value trail_12m_cost_of_matl
        ,is_operating_expn.value is_operating_expn
        ,ard_preferred_stock.value ard_preferred_stock
        ,short_and_long_term_debt.value short_and_long_term_debt
        ,cash_and_st_investments.value cash_and_st_investments
        ,cf_cash_from_oper.value cf_cash_from_oper
        ,cf_cap_expend_inc_fix_asset.value cf_cap_expend_inc_fix_asset
        ,trail_12m_minority_int.value trail_12m_minority_int 
        ,net_chng_lt_debt.value net_chng_lt_debt
    FROM t 
        LEFT JOIN t bs_sh_out 
            ON bs_sh_out.tidemark = 'bs_sh_out'
            AND bs_sh_out.asset = t.asset 
            AND bs_sh_out.date = t.date
        LEFT JOIN t trail_12m_minority_int  
            ON trail_12m_minority_int.tidemark = 'trail_12m_minority_int'
            AND trail_12m_minority_int.asset = t.asset 
            AND trail_12m_minority_int.date = t.date
        LEFT JOIN t trail_12m_net_inc_avai_com_share 
            ON trail_12m_net_inc_avai_com_share.tidemark = 'trail_12m_net_inc_avai_com_share'
            AND trail_12m_net_inc_avai_com_share.asset = t.asset 
            AND trail_12m_net_inc_avai_com_share.date = t.date
        LEFT JOIN t eps_growth 
            ON eps_growth.tidemark = 'tot_common_eqy'
            AND eps_growth.asset = t.asset 
            AND eps_growth.date = t.date
        LEFT JOIN t tot_common_eqy 
            ON tot_common_eqy.tidemark = 'tot_common_eqy'
            AND tot_common_eqy.asset = t.asset 
            AND tot_common_eqy.date = t.date
        LEFT JOIN t sales_rev_turn 
            ON sales_rev_turn.tidemark = 'sales_rev_turn'
            AND sales_rev_turn.asset = t.asset 
            AND sales_rev_turn.date = t.date
        LEFT JOIN t is_operating_expn 
            ON is_operating_expn.tidemark = 'is_operating_expn'
            AND is_operating_expn.asset = t.asset 
            AND is_operating_expn.date = t.date
        LEFT JOIN t ard_preferred_stock 
            ON ard_preferred_stock.tidemark = 'ard_preferred_stock'
            AND ard_preferred_stock.asset = t.asset 
            AND ard_preferred_stock.date = t.date
        LEFT JOIN t short_and_long_term_debt 
            ON short_and_long_term_debt.tidemark = 'short_and_long_term_debt'
            AND short_and_long_term_debt.asset = t.asset 
            AND short_and_long_term_debt.date = t.date
        LEFT JOIN t cash_and_st_investments 
            ON cash_and_st_investments.tidemark = 'cash_and_st_investments'
            AND cash_and_st_investments.asset = t.asset 
            AND cash_and_st_investments.date = t.date
        LEFT JOIN t cf_cash_from_oper 
            ON cf_cash_from_oper.tidemark = 'cf_cash_from_oper'
            AND cf_cash_from_oper.asset = t.asset 
            AND cf_cash_from_oper.date = t.date
        LEFT JOIN t cf_cap_expend_inc_fix_asset 
            ON cf_cap_expend_inc_fix_asset.tidemark = 'cf_cap_expend_inc_fix_asset'
            AND cf_cap_expend_inc_fix_asset.asset = t.asset 
            AND cf_cap_expend_inc_fix_asset.date = t.date
        LEFT JOIN t net_chng_lt_debt 
            ON net_chng_lt_debt.tidemark = 'net_chng_lt_debt'
            AND net_chng_lt_debt.asset = t.asset 
            AND net_chng_lt_debt.date = t.date
        LEFT JOIN t trail_12m_cost_of_matl 
            ON trail_12m_cost_of_matl.tidemark = 'trail_12m_cost_of_matl'
            AND trail_12m_cost_of_matl.asset = t.asset 
            AND trail_12m_cost_of_matl.date = t.date
    ORDER BY t.asset, t.date DESC
), tm_date AS (
    SELECT d.fiscal_quarter, d.fiscal_year, tp.* 
    FROM tidemark_pivot tp 
        INNER JOIN dim_date d ON d.date_key = tp.date_key
), ph_date AS (
    SELECT d.fiscal_quarter, d.fiscal_year, d.full_date, p.*
    FROM price_history p 
        INNER JOIN dim_date d ON d.full_date = p.date 
), v AS (
    SELECT t.*, p.price, p.full_date
    FROM ph_date p 
        INNER JOIN tm_date t ON t.fiscal_year = p.fiscal_year
            AND t.fiscal_quarter = p.fiscal_quarter
            AND t.asset_id = p.asset_id
)
SELECT v.asset_id, v.full_date
    ,v.price * (v.bs_sh_out) / NULLIF(v.trail_12m_net_inc_avai_com_share * v.eps_growth,0) AS best_peg_ratio

    ,v.price * (v.bs_sh_out) / NULLIF(v.trail_12m_net_inc_avai_com_share,0) AS pe_ratio

    ,v.price * (v.bs_sh_out) / NULLIF(v.tot_common_eqy,0) AS pc_to_book_ratio

    ,((v.price * v.bs_sh_out) + (v.ard_preferred_stock + v.short_and_long_term_debt + v.trail_12m_minority_int - v.cash_and_st_investments)
        ) / NULLIF(v.sales_rev_turn - v.trail_12m_cost_of_matl - v.is_operating_expn,0) AS current_ev_to_12m_ebitda

    ,v.price / NULLIF(v.cf_cash_from_oper - v.cf_cap_expend_inc_fix_asset - v.net_chng_lt_debt,0) AS px_to_free_cash_flow

    ,v.price * ((v.bs_sh_out + v.ard_preferred_stock + v.short_and_long_term_debt + v.trail_12m_minority_int - v.cash_and_st_investments)
        ) / NULLIF(v.cf_cash_from_oper - v.cf_cap_expend_inc_fix_asset - v.net_chng_lt_debt,0) AS current_ev_to_t12m_fcf

    ,v.price / NULLIF(v.sales_rev_turn,0) AS px_to_sales_ratio
FROM v);
SELECT * FROM vw_bloomberg_daily_calculated;