create or replace view TRADER_TFA_NPI_PROGRAM_PRIORITY_ORDERS(
	ORDER_ID,
	PRIORITY,
	ORDER_DT,
	SALES_ORG_DESC,
	DELIVERY_BLOCK_CD,
	DN_DROPPED,
	TRADER_CD,
	CATEGORY
) as (
SELECT * FROM  (
    SELECT orc.order_id
     , 'P2 - Incentive Review' AS Priority
     , orc.order_dt
     , CONCAT(orc.sales_org_cd, ' ', sd.description) AS sales_org_desc
     , orc.delivery_block_cd
     , case when dn.CREATE_TS is not null then 'Y' else 'N' end as DN_Dropped
     , orc.trader_cd
     , listagg(rtm.category, ',') as category
 FROM (select * from gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order
          where 1=1
          and event_dt >= CURRENT_DATE -7
          and data_salesorg in ('5600', '8400')
           and (data_updatedtradercode = 'PT' or output_probabletraderattributerule = 'true') --PT filter at ORC
          QUALIFY ROW_NUMBER () OVER (PARTITION BY data_weborder ORDER BY event_ts DESC NULLS LAST) = 1  ) wo
    LEFT JOIN gbi_fraud_semantic_db.ai.sales_org_description sd
        ON sd.sales_org_cd = wo.data_salesorg
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.read_trader_material_vw rtm
    ON rtm.mpn = TRIM(wo.data_items_id[0], '"')
  JOIN (select trader_cd, order_id, order_dt, web_order_id, sales_org_cd, delivery_block_cd
        from gbi_fraud_semantic_db.ai.order_reg_cur
        where 1=1
        and sales_org_cd in ('5600', '8400')
        and order_dt >= CURRENT_DATE -7
        and trader_cd = 'PT'
        and delivery_block_cd <> 'ST') orc
    ON orc.web_order_id = wo.data_weborder
 JOIN (select order_id, affiliate_vendor_id
       from gbi_fraud_semantic_db.ai.sales_order_hist
        where affiliate_vendor_id = '0080171595'
           and order_create_ts >= current_date -7 ) sh
    ON sh.order_id = orc.order_id
 left join (select delivery_id, ref_doc_id, create_ts
            from gbi_fraud_semantic_db.ai.delivery_item_cur
            where CREATE_TS>=CURRENT_DATE -10
            group by 1,2,3) dn
    on orc.order_id=dn.ref_doc_id
  group by 1,2,3,4,5,6,7 )

UNION

SELECT *  FROM  (
    select orc.order_id
         , 'P3 - APU Trader Hunt' AS Priority
         , po.event_dt as order_dt
         , CONCAT(po.salesorg, ' ', sd.description) AS sales_org_desc
         , orc.delivery_block_cd
         , 'N/A' AS dn_dropped
         , orc.trader_cd
         , listagg(rtm.category, ',') as category
       from (select  *
             from gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_place_order
             where event_dt >= current_date-3
             and has_apu_delivery = 'true'
             and  trim(read_trader_strategy) like '%FORCEHOMESHIP":\"Y\"%'
             and storefrontdata_countrycode in ('JP', 'HK', 'KR', 'IN')
             QUALIFY ROW_NUMBER () OVER (PARTITION BY checkoutsessionid  ORDER BY event_ts DESC NULLS LAST) = 1 ) po
       join (select *
             from gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order
             where event_dt >= current_date-3
             and output_top_level_model_type = 'ipk'
             QUALIFY ROW_NUMBER () OVER (PARTITION BY data_weborder ORDER BY event_ts DESC NULLS LAST) = 1 ) wo
           on po.checkoutsessionid = wo.output_checkoutsessionid
        LEFT JOIN gbi_fraud_semantic_db.ai.sales_org_description sd
            ON sd.sales_org_cd = po.salesorg
        JOIN gbi_fraud_bap_db.ai_live_biz_app.read_trader_material_vw rtm
            ON rtm.mpn = TRIM(wo.data_items_id[0], '"')
       join (select web_order_id, order_id, trader_cd, delivery_block_cd
             from gbi_fraud_semantic_db.ai.order_reg_cur
             where trader_cd in ('NF', 'PT', 'NA')
               and delivery_block_cd <> 'ST'
             and order_dt >= current_date-3 ) orc
             on orc.web_order_id = wo.data_weborder
        where NOT EXISTS (
                     SELECT 1
                     FROM gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_nf_to_nf_orders nf
                     WHERE nf.order_id = orc.order_id
                     )
       group by 1,2,3,4,5,6,7)

UNION

SELECT * FROM  (
    SELECT orc.order_id
     , CASE WHEN orc.sales_org_cd = '1200' THEN 'P4 - HK Trader Hunt'
            WHEN orc.sales_org_cd = '8400' THEN 'P4 - JP Trader Hunt' END AS Priority
     , orc.order_dt
     , CONCAT(orc.sales_org_cd, ' ', sd.description) AS sales_org_desc
     , orc.delivery_block_cd
     , case when dn.create_ts is not null then 'Y' else 'N' end as DN_Dropped
     , orc.trader_cd
     , listagg(rtm.category, ',') as category
 FROM (select * from gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order
          where 1=1
          and event_dt >= CURRENT_DATE -7
          and data_salesorg in ('1200', '8400')
          and data_updatedtradercode = 'NF'
          and output_top_level_model_type = 'sth'
          QUALIFY ROW_NUMBER () OVER (PARTITION BY data_weborder ORDER BY event_ts DESC NULLS LAST) = 1  ) wo
    JOIN (SELECT checkoutsessionid, get(promo_grouping.value,'MAKTX') as promo_cate
          FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_place_order a
          LEFT JOIN LATERAL FLATTEN(input => a.READ_PROMO_GROUPING,OUTER => TRUE) AS promo_grouping
          WHERE event_dt >= CURRENT_DATE -7
          AND promo_cate IS NULL  -- this join is to exclude promo order (partial ship issue)
          QUALIFY ROW_NUMBER () OVER (PARTITION BY checkoutsessionid ORDER BY event_ts DESC NULLS LAST) = 1) po
        ON po.checkoutsessionid = wo.output_checkoutsessionid
    LEFT JOIN gbi_fraud_semantic_db.ai.sales_org_description sd
        ON sd.sales_org_cd = wo.data_salesorg
JOIN gbi_fraud_bap_db.ai_live_biz_app.read_trader_material_vw rtm
    ON rtm.mpn = TRIM(wo.data_items_id[0], '"')
join (select SALES_ORG_CD, PROD_ID, TRADER_STRATEGY_CD
      from gbi_fraud_bap_db.ai_live_biz_app.trader_product_strategy_published_source
      where TRADER_STRATEGY_CD IN ('CANCEL','REQUOTE')
      group by 1,2,3) tps
    on TRIM(wo.data_items_id[0], '"')=tps.prod_id
  JOIN (select trader_cd, order_id, order_dt, web_order_id, sales_org_cd, delivery_block_cd
        from gbi_fraud_semantic_db.ai.order_reg_cur
        where 1=1
        and order_dt >= CURRENT_DATE -7
        and trader_cd = 'NF') orc
    ON orc.web_order_id = wo.data_weborder
 left join (select delivery_id, ref_doc_id, create_ts
            from GBI_FRAUD_SEMANTIC_DB.ai.DELIVERY_ITEM_CUR
            where CREATE_TS>=CURRENT_DATE -10
            group by 1,2,3) dn
    on orc.order_id=dn.ref_doc_id
 AND NOT EXISTS (
                     SELECT 1
                     FROM gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_nf_to_nf_orders nf
                     WHERE nf.order_id = orc.order_id
                     )
  group by 1,2,3,4,5,6,7 )

-- SELECT* FROM (
--     SELECT so.order_id
--      , 'P2 - DGF PT' AS Priority
--      , orc.order_dt
--      , CONCAT(so.sales_org_cd, ' ', sd.description) AS sales_org_desc
--      , so.delivery_block_cd
--      , case when dn.create_ts is not null then 'Y' else 'N' end as DN_Dropped
--      , orc.trader_cd
--      , listagg(rtm.category, ',') as category
--         FROM (select * from gbi_fraud_semantic_db.ai.sales_order
--              where 1=1
--              and doc_dt >= CURRENT_DATE -7
--              and trim(rejection_cd) = ''
--              and trader_cd ='PT'
--              and delivery_block_cd = 'GF'
--              and bopis_item_categ_cd not like '%IPK%' ) so  -- trader code delay, join ORC for up to date trader code
--     LEFT JOIN gbi_fraud_semantic_db.ai.sales_org_description sd
--         ON sd.sales_org_cd = so.sales_org_cd
-- JOIN gbi_fraud_bap_db.ai_live_biz_app.read_trader_material_vw rtm
--     ON rtm.mpn = so.prod_id
-- join (select sales_org_cd, prod_id, trader_strategy_cd
--       from gbi_fraud_bap_db.ai_live_biz_app.trader_product_strategy_published_source
--       where TRADER_STRATEGY_CD IN ('CANCEL','REQUOTE')
--       group by 1,2,3) tps
--     on so.prod_id=tps.prod_id
--     and so.sales_org_cd=tps.sales_org_cd
-- left join (select delivery_id, ref_doc_id, create_ts
--             from gbi_fraud_semantic_db.ai.delivery_item_cur
--             where CREATE_TS>= CURRENT_DATE -10
--             group by 1,2,3) dn
--     on so.ORDER_ID=dn.ref_doc_id
--  JOIN (select trader_cd, order_dt, order_id
--         from gbi_fraud_semantic_db.ai.order_reg_cur
--         where 1=1
--         and order_dt >= CURRENT_DATE -7
--         and trader_cd = 'PT'
--         group by 1,2,3) orc
--     ON orc.order_id = so.order_id
-- WHERE category is not null
--   AND DN_Dropped='N'
-- group by 1,2,3,4,5,6,7 )
--
--   UNION
--
-- SELECT* FROM (
--     SELECT so.order_id
--      , 'P2 - DGF Promo PT' AS Priority
--      , orc.order_dt
--      , CONCAT(so.sales_org_cd, ' ', sd.description) AS sales_org_desc
--      , so.delivery_block_cd
--      , case when dn.create_ts is not null then 'Y' else 'N' end as dn_dropped
--      , orc.trader_cd
--      , so.campaign_id as category
--         FROM (select * from gbi_fraud_semantic_db.ai.sales_order
--              where 1=1
--              and doc_dt >= CURRENT_DATE -7
--              and trim(rejection_cd) = ''
--              and trader_cd ='PT'
--              and delivery_block_cd = 'GF'
--              and bopis_item_categ_cd not like '%IPK%' ) so  -- trader code delay, join ORC for up to date trader code
--     LEFT JOIN gbi_fraud_semantic_db.ai.sales_org_description sd
--         ON sd.sales_org_cd = so.sales_org_cd
-- JOIN gbi_fraud_bap_db.ai_live_biz_app.read_trader_material_vw rtm
--     ON rtm.mpn = so.prod_id
-- join (select campaign_id, trader_strategy_cd
--       from gbi_fraud_bap_db.ai_live_biz_app.trader_promo_strategy_published_source
--         where trader_strategy_cd in ('CANCEL', 'REQUOTE')
--         and campaign_id is not null
--       group by 1,2) pps
--     on so.campaign_id = pps.campaign_id
-- left join (select delivery_id, ref_doc_id, create_ts
--             from gbi_fraud_semantic_db.ai.delivery_item_cur
--             where CREATE_TS>= CURRENT_DATE -10
--             group by 1,2,3) dn
--     on so.ORDER_ID=dn.ref_doc_id
--  JOIN (select trader_cd, order_dt, order_id
--         from gbi_fraud_semantic_db.ai.order_reg_cur
--         where 1=1
--         and order_dt >= CURRENT_DATE -7
--         and trader_cd = 'PT'
--         group by 1,2,3) orc
--     ON orc.order_id = so.order_id
-- WHERE category is not null
--   AND DN_Dropped='N'
-- group by 1,2,3,4,5,6,7,8 )
--
--   UNION

  );