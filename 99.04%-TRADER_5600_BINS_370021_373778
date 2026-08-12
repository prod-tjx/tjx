-- =========================================================================
-- Pattern:        TRADER_5600_BINS_370021_373778
-- Backtesting Results:       5/22/2026 - 99.04% combined CT rate over past 90 days
--                 trader_orders=248,556 | genuine_orders=2,405
-- =========================================================================


 SET (horizon)=(90);

  -- -------------------------------------------------------------------------
  -- 1. Pattern-matching orders (past 90 days)
  -- -------------------------------------------------------------------------
  CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app._pattern_hits AS
  SELECT taws.data_weborder AS web_order_id,
         taws.event_ts      AS focal_event_ts,
         taws.data_salesorg AS sales_org
    FROM gbi_fraud_bap_db.ai_live_biz_app.tfa_aos_wo_slice taws
   WHERE taws.event_dt >= current_date - $horizon
     AND ((taws.data_salesorg = '5600'
           AND array_to_string(taws.data_payment_cardbin, ', ') = '370021')
       OR (array_to_string(taws.data_payment_cardbin, ', ') = '373778'
           AND split_part(taws.data_billtoemail, '@', 2) = 'gmail.com'))
   QUALIFY ROW_NUMBER() OVER (PARTITION BY taws.data_weborder ORDER BY taws.event_ts DESC) = 1;

  -- -------------------------------------------------------------------------
  -- 2. Pull signals, derive verdict, aggregate
  -- -------------------------------------------------------------------------
  WITH
  -- 2a. Activation source: True_First_Activation_Dt from DS_UNBRICK (winner='Y').
  activation AS (
    SELECT order_id,
           MIN(true_first_activation_dt) AS true_first_activation_dt
      FROM gbi_fraud_bap_db.ai_live_biz_app.ds_unbrick
     WHERE winner = 'Y'
     GROUP BY order_id
  ),

  -- 2b. One row per pattern order with all signals.
  --     Uses fallbacks so D synth fires even when order_summary is missing:
  --       ship_dt           -> falls back to so.doc_dt
  --       true_first_act_dt -> falls back to tal.activation_dt
  labeled AS (
    SELECT so.order_id,
           so.web_order_id,
           CASE WHEN tal.label = 'T' THEN 1 ELSE 0 END                                AS t_label,
           CASE WHEN tal.label = 'G' THEN 1 ELSE 0 END                                AS g_label,
           CASE
             WHEN COALESCE(act.true_first_activation_dt, tal.activation_dt) IS NULL
              AND DATEDIFF('day', COALESCE(os.ship_dt, so.doc_dt), CURRENT_DATE) >= 21
                  THEN 1
             WHEN DATEDIFF('day',
                           COALESCE(os.ship_dt, so.doc_dt),
                           COALESCE(act.true_first_activation_dt, tal.activation_dt)) >= 21
                  THEN 1
             ELSE 0
           END                                                                        AS d_label,
           CASE WHEN so.derived_torc_ind = 'Potential to Confirmed' THEN 1 ELSE 0 END AS analyst_ct,
           CASE WHEN so.derived_torc_ind = 'Potential to Blank'     THEN 1 ELSE 0 END AS analyst_clear
      FROM gbi_fraud_semantic_db.ai.sales_order so
      JOIN gbi_fraud_bap_db.ai_live_biz_app._pattern_hits p
        ON so.web_order_id = p.web_order_id
      LEFT JOIN gbi_fraud_semantic_db.ai.order_summary os
        ON os.order_id = so.order_id
      LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_auto_label tal
        ON tal.order_id = so.order_id AND tal.winner = 'Y'
      LEFT JOIN activation act
        ON act.order_id = so.order_id
     WHERE so.doc_dt >= current_date - $horizon
  ),

  -- 2c. Verdict precedence:
  --     G locked -> (T or D or analyst_ct) -> analyst_clear -> unlabeled.
  --     D only overturns analyst_clear because G is checked first and
  --     T / analyst_ct already fall in the TRADER bucket.
  verdicts AS (
    SELECT CASE
             WHEN g_label = 1                                               THEN 'GENUINE'
             WHEN t_label = 1 OR d_label = 1 OR analyst_ct = 1              THEN 'TRADER'
             WHEN analyst_clear = 1                                         THEN 'GENUINE'
           END AS verdict
      FROM labeled
  )

  -- 2d. Final headline
  SELECT
      ROUND(SUM(CASE WHEN verdict = 'TRADER' THEN 1 ELSE 0 END) * 100.0
              / NULLIFZERO(SUM(CASE WHEN verdict IN ('TRADER','GENUINE') THEN 1 ELSE 0 END)), 2)
                                                                  AS ct_rate_pct,
      SUM(CASE WHEN verdict = 'TRADER'  THEN 1 ELSE 0 END)        AS trader_orders,
      SUM(CASE WHEN verdict = 'GENUINE' THEN 1 ELSE 0 END)        AS genuine_orders
    FROM verdicts;