create or replace view gbi_fraud_bap_db.ai_live_biz_app.trader_TFA_SL_TH_logging_vw(
	RUN_TS,
	HUNTING_PATTERN,
	QUERY_TYPE,
	WEB_ORDER_ID,
	ORDER_DT,
	TRADER_CD,
	TOTAL_ORDER_VALUE_USD,
	ORDER_ID,
	AUDIT_KEY_CD,
	CREATE_TS,
	CREATE_DT,
	USER_ID,
	NAME,
	TEAM,
	REGION,
	ACM_CD,
	EXCEPTION_TYPE_CD,
	ACTION_TAKEN,
	ACTION_TIME,
	IS_TFA,
	ETL_CREATE_TS
) as
SELECT
    h.RUN_TS,
    h.HUNTING_PATTERN,
    h.QUERY_TYPE,
    os.WEB_ORDER_ID,
    os.ORDER_DT,
    os.TRADER_CD,
    os.TOTAL_ORDER_VALUE_USD / 100 AS TOTAL_ORDER_VALUE_USD,
    tfa.*
FROM gbi_fraud_bap_db.ai_live_biz_app.trader_TFA_SL_TH_logging h
LEFT JOIN gbi_fraud_semantic_db.ai.order_summary os
    ON h.ORDER_ID = os.ORDER_ID
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.acm_trader_first_actions tfa
    ON h.ORDER_ID = tfa.ORDER_ID
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY h.ORDER_ID
    ORDER BY h.RUN_TS DESC
) = 1;


Select * from gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_aso_ssla