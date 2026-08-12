-- Pattern:        AMR_gibberish_em_a1add_velocity
-- Backtesting Results:       6/10/2026 - 99.31% combined CT rate over past 90 days
--                 trader_orders=2,889,667 | genuine_orders=19,953
-- =========================================================================
  SET (horizon)=(90);

  -- -------------------------------------------------------------------------
  -- 1. Pattern-matching orders: AMR_gibberish_em_a1add_velocity logic
  --    sourced directly from tfa_aos_wo_slice (taws) — no open-order filter.
  --    Hard INNER JOIN preserved to trader_tfa_shipto_add_velocity_vw.
  -- -------------------------------------------------------------------------
  CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app._pattern_hits AS
  SELECT taws.data_weborder AS web_order_id,
         taws.event_ts      AS focal_event_ts,
         taws.data_salesorg AS sales_org
    FROM gbi_fraud_bap_db.ai_live_biz_app.tfa_aos_wo_slice taws
    JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_shipto_add_velocity_vw a1v
      ON taws.data_weborder = a1v.web_order_id
   WHERE taws.event_dt >= current_date - $horizon
     AND taws.data_salesorg     IN ('3400', '5600')
     AND taws.data_salesdistrict = 'IN02'
     AND ( -- payment_types: same derivation the trader-hunt frame uses
           CASE WHEN taws.data_payment_actualsapcardtype IS NULL
                THEN taws.data_paymentterms
                ELSE ARRAY_TO_STRING(taws.data_payment_actualsapcardtype, ', ')
           END LIKE ANY ('AGCD%', 'APID%')
        OR SPLIT_PART(taws.data_billtoemail, '@', 2) ILIKE ANY ('%.top%', '%.uk%', '%.games%')
        OR SPLIT_PART(taws.data_billtoemail, '@', 2) IN (
             '0htmail.com','521wanli.com','570mu.com','a21.hhhrsc.com','a67.zznoo.com','angoodrm.com',
             'badck.com','bananailove.com','bmdde.com','bmdek.com','bvole.com','clwqkl.asia','cqukk.com',
             'cwkuy.com','ddiuuu.com','ducwd.com','dwadc.com','foxxmial.com','gmx.com','guaikuan.com',
             'haiyue111.com','hcddy.com','hnbad.com','hquihdj.com','htiwe9.com','hun.he.cn','hwbak.com',
             'hwdch.com','isr.hk.cn','istnsddy.com','jaccb.com','kabdy.com','klsmwl.com','kuckb.com',
             'kzuak.com','muddc.com','nbwak.com','ndcak.com','ohiouy.com','padxy.com','pddub.com',
             'piqiu668.com','piqiu778.com','qianjieli.com','sdubw.com','skwck.com','stuonewin.com',
             'stuwinwin.com','tikokk.com','tztrump.com','tzwanyu.com','usokwan.com','wbcah.com',
             'wdckk.com','xs8857.com','yackk.com','yaohan7.com','yckad.com','yyikz.com','yzyxyc.com',
             'zhouwei7.com','zjliping.com','zwtg7.com','hgfdd.com','ywkux.com','extmail.us'
           ))
     AND CASE
           WHEN LENGTH(REPLACE(taws.data_billtoname, ' ', '')) = 0
             OR taws.data_billtoemail IS NULL THEN NULL
           ELSE JAROWINKLER_SIMILARITY(LOWER(REPLACE(taws.data_billtoname, ' ', '')),
                                       LOWER(SPLIT_PART(taws.data_billtoemail, '@', 1)))
         END < 80
   QUALIFY ROW_NUMBER() OVER (PARTITION BY taws.data_weborder ORDER BY taws.event_ts DESC) = 1;

  -- -------------------------------------------------------------------------
  -- 2. Pull signals, derive verdict, aggregate (unchanged)
  -- -------------------------------------------------------------------------
  WITH
  activation AS (
    SELECT order_id,
           MIN(true_first_activation_dt) AS true_first_activation_dt
      FROM gbi_fraud_bap_db.ai_live_biz_app.ds_unbrick
     WHERE winner = 'Y'
     GROUP BY order_id
  ),

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

  verdicts AS (
    SELECT CASE
             WHEN g_label = 1                                  THEN 'GENUINE'
             WHEN t_label = 1 OR d_label = 1 OR analyst_ct = 1 THEN 'TRADER'
             WHEN analyst_clear = 1                            THEN 'GENUINE'
           END AS verdict
      FROM labeled
  )

  SELECT
      ROUND(SUM(CASE WHEN verdict = 'TRADER' THEN 1 ELSE 0 END) * 100.0
              / NULLIFZERO(SUM(CASE WHEN verdict IN ('TRADER','GENUINE') THEN 1 ELSE 0 END)), 2)
                                                                  AS ct_rate_pct,
      SUM(CASE WHEN verdict = 'TRADER'  THEN 1 ELSE 0 END)        AS trader_orders,
      SUM(CASE WHEN verdict = 'GENUINE' THEN 1 ELSE 0 END)        AS genuine_orders
    FROM verdicts;