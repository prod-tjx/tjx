-- query_type: TH
-- ============================================================================
-- Trader Hunting: ASO Backlog × Graph Components × IP Velocity
-- Rule: LARGE_COMPONENT_WITH_TRADERS = 1 AND IP_ADD_MAX_COUNT >= 30
-- Backtest accuracy: 98.11% Trader Label Rate (123,210 / 125,581 labeled)
-- Population: 2,228,353 orders over 90d lookback
-- DRI: tjx@apple.com
-- ============================================================================

CREATE OR REPLACE VIEW gbi_fraud_bap_db.ai_live_biz_app.trader_aso_backlog_ip_velocity_hunt AS (

WITH
backlog AS (
    SELECT
        b.order_id,
        b.web_order_id,
        b.sales_org_cd,
        b.sales_district_cd,
        t.salesorg      AS aso_salesorg,
        t.ipaddress     AS aso_ipaddress
    FROM gbi_fraud_bap_db.ai_live_biz_app.TRADER_TFA_ASO_ACM_BACKLOG_VW b
    LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.aso_transactions t
        ON  t.salesorder = b.order_id
        AND t.event_ts  >= CURRENT_DATE - 5
    WHERE b.event_ts        >= TO_VARCHAR(CURRENT_DATE - 5, 'YYYY-MM-DD')
      AND b.sales_district_cd LIKE '%IN%'
      AND b.sales_org_cd    <> '1320'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY b.order_id ORDER BY t.event_ts DESC) = 1
),

ip_add_raw AS (
    SELECT
        b.order_id,
        ip_add.item_count AS ip_add_item_count
    FROM backlog b
    LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_ip_add_category_counts ip_add
        ON ip_add.sales_org = b.aso_salesorg
       AND ip_add.ip_add    = b.aso_ipaddress
),

ip_add_max AS (
    SELECT
        r.order_id,
        MAX(TRY_CAST(TRIM(SPLIT_PART(f.value::STRING, ',', 1)) AS INTEGER))
            AS ip_add_max_count
    FROM ip_add_raw r,
         LATERAL FLATTEN(input => r.ip_add_item_count, OUTER => TRUE) f
    GROUP BY r.order_id
),

component_stats AS (
    SELECT DISTINCT
        COMPONENT_ID,
        LARGE_COMPONENT_WITH_TRADERS
    FROM gbi_fraud_bap_db.ai_live_biz_app.TRADER_GRAPH_ORDER_LOOKUP
),

graph AS (
    SELECT
        tg.WEB_ORDER_ID,
        cs.LARGE_COMPONENT_WITH_TRADERS
    FROM gbi_fraud_bap_db.ai_live_biz_app.TRADER_GRAPH_ORDER_LOOKUP tg
    LEFT JOIN component_stats cs
        ON cs.COMPONENT_ID = tg.COMPONENT_ID
)

SELECT DISTINCT
    b.order_id,
    b.web_order_id
FROM backlog b
LEFT JOIN graph      g   ON g.WEB_ORDER_ID = b.web_order_id
LEFT JOIN ip_add_max ipm ON ipm.order_id   = b.order_id
WHERE g.LARGE_COMPONENT_WITH_TRADERS = 1
  AND ipm.ip_add_max_count           >= 30

);
