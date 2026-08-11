create or replace view TRADER_TFA_ASO_ACM_BACKLOG_VW(
	WEB_ORDER_ID,
	ORDER_ID,
	ORDERSTATUS,
	ORDER_DT,
	EVENT_TS,
	SLA_TIME,
	CURRENT_GMT_TS,
	DUE_IN_HOUR,
	PAST_DUE_ALERT,
	COMMIT_CD,
	COMMIT_DT,
	TRADER_CD,
	DELIVERY_BLOCK_CD,
	EXCEPTION_CD,
	REGION,
	SALES_ORG_CD,
	SALES_DISTRICT_CD,
	DISCOUNT_STORE_TYPE,
	ORDER_TYPE,
	PROD_DESC,
	EVENT_SLA_GAP,
	ACTION_DESC
) as

SELECT acm.weborder                                                             AS web_order_id
     , acm.salesorder                                                           AS order_id
     , acm.orderstatus
     , acm.ordercreationdate                                                    AS order_dt
     , to_varchar(to_timestamp(acm.event_ts), 'YYYY-MM-DD HH24:MI:SS')          AS event_ts
     , to_varchar(to_timestamp(acm.sla_time), 'YYYY-MM-DD HH24:MI:SS')          AS sla_time
     , to_varchar(convert_timezone('GMT', current_timestamp(0)), 'YYYY-MM-DD HH24:MI:SS')                       AS current_GMT_ts --built-in timestamp is GMT
     , datediff(HOUR, current_GMT_ts, acm.sla_time)                             AS due_in_hour
     , CASE WHEN due_in_hour < 0 THEN 'Past Due'
            WHEN due_in_hour <= 1 THEN 'At Risk'
            WHEN due_in_hour <= 6 THEN 'EOD Sweep'
            END                                                                 AS past_due_alert
     , occ.commit_cd
     , acm.sla_time :: DATE                                                     AS commit_dt
--      , orc.trader_cd           --the null value, it could mean that this order is not inducted in gbi_fraud_semantic_db.ai.order_reg_cur yet so they are showing as empty.
     , acm.updatedtradercode                                                    AS trader_cd
     , acm.deliveryblock                                                        AS delivery_block_cd
     , acm.exception_type                                                       AS exception_cd
     , acm.region --- seems like this column is updated now
     , acm.salesorg                                                                                          AS sales_org_cd
     , acm.salesdistrict                                                                                     AS sales_district_cd
     , CASE WHEN acm.salesdistrict IN ('IN02','IN04','RW02') THEN 'consumer'
            WHEN acm.salesdistrict IN ('IN01','RW01') THEN 'epp_primary'
            WHEN acm.salesdistrict IN ('IN20','RW20') THEN 'epp_secondary'
            WHEN acm.salesdistrict IN ('IN21','RW21') THEN 'qpromo'
            WHEN acm.salesdistrict IN ('IN15','RW15') THEN 'biz_or_gov'
            WHEN acm.salesdistrict IN ('IN05','RW05') THEN 'smb'
            WHEN acm.salesdistrict IN ('IN17','IN18','IN19','IN25','IN26','IN27','RW17','RW18','RW19','RW26','RW27') THEN 'edu'
            WHEN acm.salesdistrict IN ('RK01','RK02') THEN 'retail_kiosk'
            END                                                                                              AS discount_store_type
     , CASE WHEN array_to_string(to_variant(acm.items_bkitemflag), ',') LIKE '%IPK%' THEN 'IPK'
            WHEN array_to_string(to_variant(acm.items_bkitemflag), ',') LIKE '%IDL%' THEN 'IDL'
            WHEN acm.order_type = 'UCDGTORDDT' THEN 'Long_UCD'
            ELSE acm.order_type
            END                                                                                              AS order_type

     , array_to_string(to_variant(acm.items_description),', ')                                               AS prod_desc
     , datediff(HOUR, acm.event_ts, acm.sla_time)                                                            AS event_sla_gap
     , acm.resolution_desc                                                                                   AS action_desc
-- FROM gbi_fraud_bap_db.ai_live_biz_app.aso_transactions acm
FROM ( SELECT * FROM gbi_fraud_bap_db.ai_live_biz_app.aso_transactions
                WHERE 1=1
                AND event_ts >= current_date -30
                QUALIFY ROW_NUMBER() OVER (PARTITION BY salesorder ORDER BY modified_at DESC NULLS LAST) = 1   ) acm  -- for filter ouy duplicate order_id
LEFT JOIN ( SELECT order_id
                 , min(upper_commit_dt) AS upper_commit_dt      --use MIN to get earliest UCD
