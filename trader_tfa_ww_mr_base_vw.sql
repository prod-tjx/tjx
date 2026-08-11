--Trader MR Base view creation, Draft created on 16 Apirl 2025
-- adding Replay_ind and case_note - Feb 13, 2026
-- Move from tfa-playbook/trader/mr_revisit_2025/MR_Base_2025.sql  - 10 Mar, 2026

--1) Listing down columns that we want in a view. View name=gbi_fraud_bap_db.ai_live_biz_app.ww_trader_mr_base_new
USE ROLE GBI_FRAUD_SDS_ANALYTICS_CUST_MAIN_ROLE;

CREATE OR REPLACE VIEW gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_ww_mr_base_vw AS(

WITH aos_acc AS
    (SELECT
            aos.person_locale_cd                                  AS its_locale
--           , aos.id_creation_dt                                    AS acct_dt
          , DATEDIFF(DAY, aos.id_creation_dt, CURRENT_DATE)       AS its_age
--           , CASE WHEN MONTH(GETDATE()) > MONTH(acct_dt) OR (MONTH(GETDATE()) = MONTH(acct_dt) AND DAY(GETDATE()) >= DAY(acct_dt))
--                  THEN DATEDIFF(year, acct_dt, GETDATE())
--                  ELSE DATEDIFF(year, acct_dt, GETDATE()) -1
--                                                             END    AS its_age_in_yrs
          , taws.data_weborder AS web_order_id
          , CASE WHEN taws.data_salesorg IN ('1430', '1498', '6400', '1200', '1300', '1830', '8400', '1398')
                 THEN aos.person_last_name || ' ' || aos.person_first_name
                 ELSE aos.person_first_name || ' ' || aos.person_last_name
            END AS its_acct_nm
          ,aos.billing_first_name || ' ' || aos.billing_last_name  AS its_b2_nm
          ,aos.email_addr_txt                                      AS its_b2_em
          ,aos.person_id                                           AS person_id
     FROM gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_aos_acc AS aos
     JOIN gbi_fraud_bap_db.ai_live_biz_app.tfa_aos_wo_slice taws
       ON LOWER(TRIM(taws.data_billtoemail)) = LOWER(TRIM(aos.email_addr_txt))
     WHERE 1=1
       AND taws.event_dt >= CURRENT_DATE - 30
       AND taws.data_weborder ilike 'W%'
),
    Replay_ind AS ( SELECT salesorder
                        , CASE WHEN athena_response_status = 'REPLAY' AND ads_decision_response_status <> 'SUCCESS'
                                    THEN 'Y'
                                    ELSE 'N'
                                END AS replay_ind
                         , review_status
                         , last_actioned_by
                      FROM ( SELECT event_ts
                                  , salesorder
                                  , weborder
                                  , salesorg
                                  , exception_type
                                  , deliveryblock
                                  , ads_decision_response_status
                                  , athena_response_status
                                  , review_status
                                  , last_actioned_by

                               FROM gbi_fraud_bap_db.ai_live_biz_app.aso_transactions
                              WHERE 1 = 1
                                AND event_ts >= CURRENT_DATE - 14
                                AND deliveryblock = 'ST'                     --- get open orders
                                AND athena_response_status = 'REPLAY'
                                AND ads_decision_response_status <> 'SUCCESS'---exclude SUCCESS, replay has been resolved
                                AND exception_type NOT IN ('FA1', 'DTH')
                                AND review_status LIKE 'TRADER%' )
                      ),


     ACM_note as
         ( Select ag.order_id
            , LISTAGG(ad.new_val, ',') WITHIN GROUP (ORDER BY ad.etl_create_ts) AS case_note
           FROM (Select new_val, etl_create_ts, audit_key_cd
            from gbi_fraud_semantic_db.ai.acm_admin_actions
                where etl_create_ts >= current_date -14
                 AND lob_name = 'AOS'
                 AND field_name = 'caseNoteText'
                 AND new_val ilike 'SDS TRADER%') ad
         JOIN (Select create_ts, order_id, user_id, audit_key_cd
            from gbi_fraud_semantic_db.ai.acm_agent_actions
                where create_ts >= current_date -14
                        AND user_id <> 'system') ag
              ON ad.audit_key_cd = ag.audit_key_cd
                group by 1)

SELECT wtb.order_id                                                                            AS order_id
     , taws.data_weborder                                                                      AS web_order_id
     , taws.event_ts                                                                           AS event_ts
     , omd.upper_commit_dt                                                                     AS ucd
     , omd.commit_cd                                                                           AS commit_cd
     , wtb.exception_type_cd                                                                   AS exception_cd
     , taws.data_region                                                                        AS region
     , taws.data_potype                                                                        AS po_type
     , taws.output_order_type                                                                  AS SLA --good to exlude pup, egc, adp, other
     , wtb.sales_org_cd                                                                        AS sales_org
     , wtb.sales_district_cd                                                                   AS sales_district
     , CASE WHEN wtb.sales_district_cd IN ('IN02','RW02') THEN 'consumer'
            WHEN wtb.sales_district_cd IN ('IN01','RW01') THEN 'epp_primary'
            WHEN wtb.sales_district_cd IN ('IN20','RW20') THEN 'epp_secondary'
            WHEN wtb.sales_district_cd IN ('IN21','RW21') THEN 'qpromo'
            WHEN wtb.sales_district_cd IN ('IN15','RW15') THEN 'biz_or_gov'
            WHEN wtb.sales_district_cd IN ('IN05','RW05') THEN 'smb'
            WHEN wtb.sales_district_cd IN ('IN17','IN18','IN19','IN25','IN26','IN27','RW17','RW18','RW19','RW26','RW27') THEN 'edu'
            END                                                                                AS discount_store_type
     , taws.data_items_shiptocompany                                                           AS biz_ars_pup
     , awo_item.campaign_id                                                                    AS promo_grouping
     , awo_item.npi_edu_promo                                                                  AS npi_edu_promo
     , dn.partially_shipped                                                                    AS partially_shipped
     , taws.data_billtoname                                                                    AS b2_nm
     , aos_acc.its_acct_nm                                                                     AS its_acct_nm
     , aos_acc.its_b2_nm                                                                       AS its_b2_nm
     , CASE WHEN (UPPER(b2_nm) = UPPER(its_acct_nm)) THEN 1 ELSE 0 END                         AS nm_match
     , taws.data_personid                                                                      AS dsid
     , aos_acc.person_id                                                                       AS person_id
     , po.TRADER_OUT_OF_COUNTRY_ACTIVATION_MAP                                                 AS OOCA
     , taws.data_billtoemail                                                                   AS b2_em
     , SPLIT_PART(taws.data_billtoemail, '@', 2)                                               AS b2_em_dom
     , aos_acc.its_b2_em                                                                       AS its_b2_em
     , taws.output_account_age                                                                 AS acct_age -- need to check about the create_ts
     , aos_acc.its_age                                                                         AS its_age
     , aos_acc.its_locale                                                                      AS its_locale
     , CASE
             WHEN taws.data_salesorg IN ('1398', '1430', '1480', '1498', '1830', '2040','8400', '1200', '1300','6400')
             THEN TRIM(CONCAT(COALESCE (taws.data_billtostreetprefix, '')
                       , ' ', COALESCE (taws.data_billtostreet, '')
                       , ' ', COALESCE (taws.data_billtocompany, '')
                       , ' ', COALESCE (taws.data_billtocity, '')))
             ELSE TRIM(CONCAT(COALESCE (taws.data_billtostreet, '')
                       , ' ', COALESCE (taws.data_billtostreetprefix, '')
                       , ' ', COALESCE (taws.data_billtocompany, '')
                       , ' ', COALESCE (taws.data_billtocity, '')
                       , ' ', COALESCE (taws.data_billtostate, '')))
             END                                                                               AS b2_add
     , taws.data_billtocity                                                                    AS b2_city
     , taws.data_billtodistrict                                                                AS b2_district
     , taws.data_billtostate                                                                   AS b2_state
     , taws.data_billtozip                                                                     AS b2_zip
     , taws.data_billtophonenumber                                                             AS b2_ph
     , taws.data_shiptoname                                                                    AS a1_nm
     , taws.data_shiptoemail                                                                   AS a1_em
     , SPLIT_PART(taws.data_shiptoemail, '@', 2)                                               AS a1_em_dom
     , CASE WHEN taws.data_salesorg IN ('1398', '1430', '1480', '1498', '1830','2040','8400','1200', '1300','6400')
             THEN TRIM(CONCAT(COALESCE (taws.data_shiptostreetprefix, '')
                       , ' ', COALESCE (taws.data_shiptostreet, '')
                       , ' ', COALESCE (taws.data_shiptocompany, '')
                       , ' ', COALESCE (taws.data_shiptocity, '')))
             ELSE TRIM(CONCAT(COALESCE (taws.data_shiptostreet, '')
                       , ' ', COALESCE (taws.data_shiptostreetprefix, '')
                       , ' ', COALESCE (taws.data_shiptocompany, '')
                       , ' ', COALESCE (taws.data_shiptocity, '')
                       , ' ', COALESCE (taws.data_shiptostate, '')))
            END                                                                                AS a1_add
     , taws.OUTPUT_APPLE_MAPS_NORMALIZED_ADDRESS                                               AS apple_maps_add
     , taws.output_apple_maps_normalized_address_unit_stripped                                 AS apple_maps_ff -- matching with FF table
     , CASE WHEN ff.output_apple_maps_normalized_address_unit_stripped IS NOT NULL
        THEN 1 ELSE 0 END                                                                      AS ff_ind
     , taws.data_shiptocity                                                                    AS a1_city
     , taws.data_shiptodistrict                                                                AS a1_district
     , taws.data_shiptostate                                                                   AS a1_state
     , taws.data_shiptozip                                                                     AS a1_zip
     , taws.data_shiptophonenumber                                                             AS a1_phone
     , TRIM(taws.data_items_shiptoname[0],'')                                                  AS latest_a1_nm
     , TRIM(taws.data_items_shiptoemail[0],'')                                                 AS latest_a1_em
     , CASE WHEN taws.data_salesorg IN ('1398', '1430', '1480', '1498', '1830', '2040', '8400', '1200', '1300', '6400')
             THEN TRIM(CONCAT(COALESCE (taws.data_items_shiptostreetprefix[0],'')
                       , ' ', COALESCE (taws.data_items_shiptostreet[0],'')
                       , ' ', COALESCE (taws.data_items_shiptocity[0],'')))
             ELSE TRIM(CONCAT(COALESCE (taws.data_items_shiptostreet[0],'')
                       , ' ', COALESCE (taws.data_items_shiptostreetprefix[0],'')
                       , ' ', COALESCE (taws.data_items_shiptocity[0],'')
                       , ' ', COALESCE (taws.data_items_shiptostate[0],'')))
            END                                                                           AS latest_a1_add
     , TRIM(taws.data_items_shiptocity[0],'')                                             AS latest_a1_city
     , TRIM(taws.data_items_shiptostate[0],'')                                            AS latest_a1_state
     , TRIM(taws.data_items_shiptozip[0],'')                                              AS latest_a1_zip
     , taws.data_items_thirdpartyname                                                     AS latest_bopis3_nm
      ,taws.data_items_thirdpartyemail                                                    AS latest_bopis3_em
     , po.sessionidentifier_milvetid                                                      AS student_id
     , ROUND(CAST (taws.output_amt AS BIGINT) / 100.00, 2)                                AS total_value
     , awo_item.item_current_count                                                        AS current_category_count
     , awo_item.prods                                                                     AS prod_desc
     , taws.data_items_giftingtext                                                        AS gift_text
     , CONCAT( CASE WHEN taws.data_items_updatedengravingtext1 IS NOT NULL THEN 1 ELSE 0 END, '-',
               CASE WHEN ARRAY_TO_STRING(taws.data_items_id,',') LIKE ANY ('Z1%','%,Z1%') THEN 1 ELSE 0 END )
                                                                                               AS engr_cto
     , CASE WHEN taws.data_ipaddress LIKE ANY ('17.%','144.178.%','10.82.%')
            THEN 'apple_ip' ELSE taws.data_ipaddress  END                                      AS ip_add
     , ARRAY_TO_STRING(ARRAY_SLICE(STRTOK_TO_ARRAY(taws.data_ipaddress, '.'), 0,3), '.')       AS ip_class
     , taws.output_geoloc_ip_carrier                                                           AS ip_company
     , taws.output_geoloc_ip_countrycode                                                       AS ip_country
     , taws.data_pcprint                                                                       AS guid
     , taws.data_sessioncookie                                                                 AS session_cookie
     , taws.data_payment_cardbin                                                               AS card_bin
     , taws.output_pmt_cardbin_country3alpha                                                   AS bin_country
     , TRIM(taws.data_payment_cardnumberhash[0], '"')                                          AS card_hash
     , taws.output_pmt_cardbin_issuer_name                                                     AS card_issuer
     , CASE WHEN taws.data_payment_actualsapcardtype IS NULL
            THEN to_array(taws.data_paymentterms) ELSE taws.data_payment_actualsapcardtype END AS payment_types
     ,opi.output_payment_id                                                                    AS payment_id
     ,CAST(ROUND(100.0 * po.apid_agcd_ratio, 2) AS NUMBER)                                     AS apid_agcd_ratio
     , replay_ind.replay_ind
     , replay_ind.review_status
     , replay_ind.last_actioned_by
     , ACM_note.case_note

--Tables that the columns will be derived from: Join main wtb with taws, omd, kanapo, aos_acc, awo_item
 FROM gbi_fraud_bap_db.ai_live_biz_app.ww_trader_backlog wtb
 LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.tfa_aos_wo_slice taws
        ON wtb.web_order_id = taws.data_weborder
 LEFT JOIN ( SELECT apo.checkoutsessionid
                  , apo.event_dt
                  , apo.trader_out_of_country_activation_map
                  , apo.apid_agcd_ratio
                  , ps.sessionidentifier_milvetid
               FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_place_order apo
               LEFT JOIN gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_place_order_short_retn ps
                      ON apo.tracker_id = ps.tracker_id
                     and apo.event_dt = ps.event_dt
              WHERE apo.event_dt >= CURRENT_DATE - 30
             QUALIFY ROW_NUMBER() OVER (PARTITION BY apo.checkoutsessionid ORDER BY apo.event_dt desc) = 1
           ) po
        ON taws.output_checkoutsessionid = po.checkoutsessionid
 LEFT JOIN ( SELECT web_order_id
                  , order_id
                  , upper_commit_dt
                  , commit_cd
               FROM gbi_fraud_bap_db.ai_live_biz_app.aos_tfa_order_mapping_60d ) omd
        ON wtb.web_order_id = omd.web_order_id
 LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.tfa_aso_freight_forwarder_address_log ff
            on ff.output_apple_maps_normalized_address_unit_stripped = taws.output_apple_maps_normalized_address_unit_stripped
 LEFT JOIN aos_acc
        ON wtb.web_order_id = aos_acc.web_order_id
 LEFT JOIN Replay_ind
        on wtb.order_id = Replay_ind.salesorder
 LEFT JOIN ACM_note
        on wtb.order_id = ACM_note.order_id
 LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_awo_items_vw awo_item
        ON wtb.web_order_id = awo_item.web_order_id
 LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_partially_shipped_vw dn
        ON wtb.order_id = dn.order_id
 LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.original_payment_id opi
        ON wtb.web_order_id = opi.data_weborder
);