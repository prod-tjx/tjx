-- Move from tfa-playbook/trader/mr_revisit_2025/acm_mr_base_backup.sql  - 10 Mar, 2026


-- This view uses gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_aos_acc.
-- It should be used while gbi_fraud_semantic_db.ai.order_reg_cur encounter significant delay.

USE ROLE GBI_FRAUD_SDS_ANALYTICS_CUST_MAIN_ROLE;

CREATE OR REPLACE VIEW gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_ww_acm_mr_base_vw AS(

WITH aos_acc AS
    (SELECT
            aos.person_locale_cd                                  AS its_locale
--           , aos.id_creation_dt                                    AS acct_dt
          , DATEDIFF(DAY, aos.id_creation_dt, CURRENT_DATE)       AS its_age
--           , CASE WHEN MONTH(GETDATE()) > MONTH(acct_dt) OR (MONTH(GETDATE()) = MONTH(acct_dt) AND DAY(GETDATE()) >= DAY(acct_dt))
--                  THEN DATEDIFF(year, acct_dt, GETDATE())
--                  ELSE DATEDIFF(year, acct_dt, GETDATE()) -1
--                                                             END    AS its_age_in_yrs
          , acm.weborder                                          AS web_order_id
          , CASE WHEN acm.salesorg IN ('1430', '1498', '6400', '1200', '1300', '1830', '8400', '1398')
                 THEN aos.person_last_name || ' ' || aos.person_first_name
                 ELSE aos.person_first_name || ' ' || aos.person_last_name
            END AS its_acct_nm
          ,aos.billing_first_name || ' ' || aos.billing_last_name  AS its_b2_nm
          ,aos.email_addr_txt                                      AS its_b2_em
          ,aos.person_id                                           AS person_id

     FROM gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_aos_acc aos
     JOIN gbi_fraud_bap_db.ai_live_biz_app.aso_transactions acm
       ON LOWER(TRIM(acm.billtoemail)) = LOWER(TRIM(aos.email_addr_txt))
     WHERE 1=1
       AND acm.event_ts >= CURRENT_DATE - 30
       AND acm.orderstatus = 'OPEN'
       AND acm.deliveryblock = 'ST'
       AND acm.exception_type NOT IN ('FA1', 'DTH')
       AND acm.orderstatus <> 'CLOSED'
)

SELECT acm.salesorder                                                                          AS order_id
     , acm.weborder                                                                            AS web_order_id
     , TO_VARCHAR(TO_TIMESTAMP(acm.event_ts), 'YYYY-MM-DD HH24:MI:SS')                         AS event_ts
     , TO_VARCHAR(CONVERT_TIMEZONE('GMT', CURRENT_TIMESTAMP(0)), 'YYYY-MM-DD HH24:MI:SS')      AS current_ts
     , omd.upper_commit_dt                                                                     AS ucd
     , omd.commit_cd                                                                           AS commit_cd
     , acm.exception_type                                                                      AS exception_cd
     , acm.region                                                                              AS region
     , acm.potype                                                                              AS po_type
     , CASE WHEN ARRAY_TO_STRING(TO_VARIANT(acm.items_bkitemflag), ',') LIKE '%IPK%'
                 THEN 'IPK'
             WHEN ARRAY_TO_STRING(TO_VARIANT(acm.items_bkitemflag), ',') LIKE '%IDL%'
                 THEN 'IDL'
             WHEN acm.order_type = 'UCDGTORDDT'
                 THEN 'Long_UCD'
                    ELSE acm.order_type     END                                                AS sla
     , acm.salesorg                                                                            AS sales_org
     , acm.salesdistrict                                                                       AS sales_district
     , CASE WHEN acm.salesdistrict IN ('IN02','RW02') THEN 'consumer'
            WHEN acm.salesdistrict IN ('IN01','RW01') THEN 'epp_primary'
            WHEN acm.salesdistrict IN ('IN20','RW20') THEN 'epp_secondary'
            WHEN acm.salesdistrict IN ('IN21','RW21') THEN 'qpromo'
            WHEN acm.salesdistrict IN ('IN15','RW15') THEN 'biz_or_gov'
            WHEN acm.salesdistrict IN ('IN05','RW05') THEN 'smb'
            WHEN acm.salesdistrict IN ('IN17','IN18','IN19','IN25','IN26','IN27','RW17','RW18','RW19','RW26','RW27') THEN 'edu'
            END                                                                                AS discount_store_type
     , acm.items_shiptocompany[0]                                                              AS biz_ars_pup
     , awo_item.campaign_id                                                                    AS promo_grouping
     , awo_item.npi_edu_promo                                                                  AS npi_edu_promo
     , dn.partially_shipped                                                                    AS partially_shipped
     , acm.billtoname                                                                          AS b2_nm
     , aos_acc.its_acct_nm                                                                     AS its_acct_nm
     , aos_acc.its_b2_nm                                                                       AS its_b2_nm
     , CASE WHEN (UPPER(b2_nm) = UPPER(its_acct_nm)) THEN 1 ELSE 0 END                         AS nm_match
     , acm.personid                                                                            AS dsid
     , aos_acc.person_id                                                                       AS person_id
     , po.TRADER_OUT_OF_COUNTRY_ACTIVATION_MAP                                                 AS OOCA
     , acm.billtoemail                                                                         AS b2_em
     , SPLIT_PART(acm.billtoemail, '@', 2)                                                     AS b2_em_dom
     , aos_acc.its_b2_em                                                                       AS its_b2_em
     , taws.output_account_age                                                                 AS acct_age -- need to check about the create_ts
     , aos_acc.its_age                                                                         AS its_age
     , aos_acc.its_locale                                                                      AS its_locale
     , acm.billtoaddress                                                                       AS b2_add
     , acm.billtocity                                                                          AS b2_city
     , acm.billtodistrict                                                                      AS b2_district
     , acm.billtostate                                                                         AS b2_state
     , acm.billtozip                                                                           AS b2_zip
     , acm.billtophonenumber                                                                   AS b2_ph
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
     , taws.data_shiptocity                                                                    AS a1_city
     , taws.data_shiptodistrict                                                                AS a1_district
     , taws.data_shiptostate                                                                   AS a1_state
     , taws.data_shiptozip                                                                     AS a1_zip
     , taws.data_shiptophonenumber                                                             AS a1_phone
     , TRIM(acm.items_shiptoname[0],'')                                                        AS latest_a1_nm
     , TRIM(acm.items_shiptoemail[0],'')                                                       AS latest_a1_em
     , TRIM(acm.items_shiptoaddress[0],'')                                                     AS latest_a1_add
     , TRIM(acm.items_shiptocity[0],'')                                                        AS latest_a1_city
     , TRIM(acm.items_shiptostate[0],'')                                                       AS latest_a1_state
     , TRIM(acm.items_shiptozip[0],'')                                                         AS latest_a1_zip
     , taws.data_items_thirdpartyname                                                     AS latest_bopis3_nm
      ,taws.data_items_thirdpartyemail                                                    AS latest_bopis3_em
     , po.sessionidentifier_milvetid                                                      AS student_id
     , acm.totalordervalue                                                                AS total_value
     , awo_item.item_current_count                                                        AS current_category_count
     , awo_item.prods                                                                     AS prod_desc
     , taws.data_items_giftingtext                                                        AS gift_text
     , CONCAT(CASE WHEN ARRAY_TO_STRING(TO_VARIANT(acm.items_engravingflag), ',') LIKE '%Y%'
                        THEN '1'
                        ELSE '0'
                    END, '-', CASE WHEN ARRAY_TO_STRING(TO_VARIANT(acm.items_id), ',') LIKE ANY ('Z1%', '%,Z1%')
                                       THEN '1'
                                       ELSE '0'
                                   END)                                                        AS engr_cto
     , CASE WHEN acm.ipaddress LIKE ANY ('17.%','144.178.%','10.82.%')
            THEN 'apple_ip' ELSE acm.ipaddress  END                                            AS ip_add
     , ARRAY_TO_STRING(ARRAY_SLICE(STRTOK_TO_ARRAY(acm.ipaddress, '.'), 0,3), '.')             AS ip_class
     , taws.output_geoloc_ip_carrier                                                           AS ip_company
     , taws.output_geoloc_ip_countrycode                                                       AS ip_country
     , acm.pcprint                                                                             AS guid
     , acm.sessioncookie                                                                       AS session_cookie
     , taws.data_payment_cardbin                                                               AS card_bin
     , taws.output_pmt_cardbin_country3alpha                                                   AS bin_country
     , TRIM(acm.payment_cardnumberhash[0], '"')                                                AS card_hash
     , taws.output_pmt_cardbin_issuer_name                                                     AS card_issuer
     , CASE WHEN taws.data_payment_actualsapcardtype IS NULL
            THEN to_array(taws.data_paymentterms) ELSE taws.data_payment_actualsapcardtype END AS payment_types
     ,opi.output_payment_id                                                                    AS payment_id
     ,CAST(ROUND(100.0 * po.apid_agcd_ratio, 2) AS NUMBER)                                     AS apid_agcd_ratio

--Tables that the columns will be derived from: Join main wtb with taws, omd, kanapo, aos_acc, awo_item
 FROM gbi_fraud_bap_db.ai_live_biz_app.aso_transactions acm
 LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.tfa_aos_wo_slice taws
        ON acm.weborder = taws.data_weborder
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
        ON acm.weborder = omd.web_order_id
 LEFT JOIN aos_acc
        ON acm.weborder = aos_acc.web_order_id
 LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_awo_items_vw awo_item
        ON acm.weborder = awo_item.web_order_id
 LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_partially_shipped_vw dn
        ON acm.weborder = dn.order_id
 LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.original_payment_id opi
        ON acm.weborder = opi.data_weborder
WHERE acm.event_ts >= CURRENT_DATE - 30
             AND acm.orderstatus = 'OPEN'
             AND acm.deliveryblock = 'ST'
             AND acm.exception_type NOT IN ('FA1', 'DTH')
             AND acm.orderstatus <> 'CLOSED'
);