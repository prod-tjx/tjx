-- ================================================================================
-- TRADER HUNTING — CONSOLIDATED PATTERN DDL EXPORT
-- ================================================================================
-- Generated:  2026-04-15 23:04:28 UTC
-- Scope:      ACTIVE patterns only
-- Total:      10  (GCT=3, TH=7)
-- Source:     gbi_fraud_bap_db.ai_live_biz_app.TRADER_HUNTING_PATTERNS
--
-- Auto-generated — re-run the script to refresh from Snowflake.
-- ==============================================================================

-- TABLE OF CONTENTS
--
-- [TH] Trader Hunting Patterns (7)
--     - AMR_gibberish_em_a1add_velocity
--     - TRADER_5600_BINS_370021_373778
--     - TRADER_5600_BIZ_GOV_457079
--     - TRADER_AMR_Common_EM_DOM
--     - TRADER_AMR_UNIQUE_EM_DOM
--     - trader_5600_bizgov_bin_512068
--     - trader_apid_scriptor_low_match
--
-- [GCT] Good Customer Patterns (3)
--     - TRADER_FREIGHT_FORWARDER
--     - trader_genuine_customer_orders
--     - trader_good_product_set
--

-- ================================================================================
-- SECTION: TH — Trader Hunting Patterns  (7 patterns)
-- ================================================================================

-- ------------------------------------------------------------------------------
-- Pattern:    AMR_gibberish_em_a1add_velocity
-- Type:       TH (Trader Hunting Patterns)
-- Activity:   Active
-- Created by: Anita Temperli
-- Created:    2025-09-12 03:34:03.885000
-- Columns (17): ORDER_ID, WEB_ORDER_ID, SALES_ORG, B2_NM, EMAIL_HANDLE, B2_EM_DOM, COMMON_EM_DOM_IND, NM2EM_MATCH_SCORE, A1_ADD, APPLE_MAP_ADD, LATEST_A1_ADD, PROD_DESC, PAYMENT_TYPES, A1_ADD_HVP, APPLE_MAP_HVP, LATEST_A1_HVP, ADD_EXCEED_IND_60D
-- ------------------------------------------------------------------------------
create or replace view AMR_GIBBERISH_EM_A1ADD_VELOCITY(
	ORDER_ID,
	WEB_ORDER_ID,
	SALES_ORG,
	B2_NM,
	EMAIL_HANDLE,
	B2_EM_DOM,
	COMMON_EM_DOM_IND,
	NM2EM_MATCH_SCORE,
	A1_ADD,
	APPLE_MAP_ADD,
	LATEST_A1_ADD,
	PROD_DESC,
	PAYMENT_TYPES,
	A1_ADD_HVP,
	APPLE_MAP_HVP,
	LATEST_A1_HVP,
	ADD_EXCEED_IND_60D
) as 
WITH apid_scriptor_base AS ( SELECT thf.order_id
                                  , thf.web_order_id
                                  , thf.sales_org
                                  , LOWER(REPLACE(b2_nm, ' ', ''))   AS b2_nm
                                  , LOWER(SPLIT_PART(b2_em, '@', 1)) AS email_handle
                                  , SPLIT_PART(b2_em, '@', 2)        AS b2_em_dom
                                  -- Using Jaro-Winkler similarity to compare cleaned name with email handle
                                  , CASE WHEN b2_em_dom IN
                                              ('163.com', 'aol.com', 'gmail.com', 'hotmail.com', 'icloud.com',
                                               'mac.com', 'me.com', 'outlook.com', 'qq.com', 'yahoo.com')
                                             THEN 1
                                             ELSE 0
                                         END                         AS common_em_dom_ind
                                  , CASE WHEN LENGTH(REPLACE(b2_nm, ' ', '')) = 0 OR b2_em IS NULL
                                             THEN NULL
                                             ELSE JAROWINKLER_SIMILARITY(LOWER(REPLACE(b2_nm, ' ', '')),
                                                                         LOWER(SPLIT_PART(b2_em, '@', 1)))
                                         END                         AS nm2em_match_score
                                  , a1v.a1_add
                                  , a1v.apple_map_add
                                  , a1v.latest_a1_add
                                  , thf.prod_desc
                                  , thf.PAYMENT_TYPES
                                  , a1v.a1_add_hvp
                                  , a1v.apple_map_hvp
                                  , a1v.latest_a1_hvp
                                  , a1v.Add_exceed_ind_60d
                               FROM (Select * from gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_traderhunt_frame
                                            where sales_org IN ('3400', '5600')) thf
                               JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_shipto_add_velocity_vw a1v
                                   ON thf.web_order_id = a1v.web_order_id
                              WHERE 1 = 1
                                AND sales_district = 'IN02'
                                AND payment_types LIKE ANY ('AGCD%', 'APID%')
    Or b2_em_dom ilike any ('%.top%', '%.uk%','%.games%')
    or b2_em_dom in ('0htmail.com','521wanli.com','570mu.com','a21.hhhrsc.com','a67.zznoo.com','angoodrm.com',
                    'badck.com','bananailove.com','bmdde.com','bmdek.com','bvole.com','clwqkl.asia','cqukk.com','cwkuy.com','ddiuuu.com',
                    'ducwd.com','dwadc.com','foxxmial.com','gmx.com','guaikuan.com','haiyue111.com','hcddy.com'
                    ,'hnbad.com','hquihdj.com','htiwe9.com','hun.he.cn','hwbak.com','hwdch.com','isr.hk.cn','istnsddy.com','jaccb.com',
                    'kabdy.com','klsmwl.com','kuckb.com','kzuak.com','muddc.com','nbwak.com','ndcak.com','ohiouy.com','padxy.com','pddub.com',
                    'piqiu668.com','piqiu778.com','qianjieli.com','sdubw.com','skwck.com','stuonewin.com','stuwinwin.com','tikokk.com',
                    'tztrump.com','tzwanyu.com','usokwan.com','wbcah.com','wdckk.com','xs8857.com','yackk.com','yaohan7.com','yckad.com',
                    'yyikz.com','yzyxyc.com','zhouwei7.com','zjliping.com','zwtg7.com', 'hgfdd.com', 'ywkux.com', 'extmail.us'))

SELECT *
FROM apid_scriptor_base
 where nm2em_match_score <  80;

-- ------------------------------------------------------------------------------
-- Pattern:    TRADER_5600_BINS_370021_373778
-- Type:       TH (Trader Hunting Patterns)
-- Activity:   Active
-- Created by: Stefanie Brabec
-- Created:    2025-09-24 20:58:07.527000
-- Columns (53): ORDER_ID, WEB_ORDER_ID, EVENT_TS, COMMIT_CD, COMMIT_DT, SALES_ORG, PO_TYPE, SALES_DISTRICT, DISCOUNT_STORE_TYPE, SLA, EXCEPTION_CD, B2_NM, B2_EM, B2_EM_DOM, ACCT_AGE, B2_ADD, B2_CITY, B2_DISTRICT, B2_STATE, B2_ZIP, B2_PH, A1_NM, A1_EM, A1_EM_DOM, A1_ADD, A1_CITY, A1_DISTRICT, A1_STATE, A1_ZIP, APPLE_MAPS_ADD, A1_PHONE, LATEST_A1_NM, LATEST_A1_EM, LASTEST_A1_EM_DOM, LATEST_A1_ADD, LATEST_A1_STATE, LATEST_A1_ZIP, LATEST_BOPIS3_NM, LATEST_BOPIS3_EM, PROD_DESC, TOTAL_VALUE, IP_ADD, IP_CLASS, IP_COMPANY, IP_COUNTRY, IP_BIN_COUNTRY, PAYMENT_TYPES, CARD_BIN, BIN_COUNTRY, CARD_HASH, CARD_ISSUER, GUID, SESSION_COOKIE
-- ------------------------------------------------------------------------------
create or replace view TRADER_5600_BINS_370021_373778(
	ORDER_ID,
	WEB_ORDER_ID,
	EVENT_TS,
	COMMIT_CD,
	COMMIT_DT,
	SALES_ORG,
	PO_TYPE,
	SALES_DISTRICT,
	DISCOUNT_STORE_TYPE,
	SLA,
	EXCEPTION_CD,
	B2_NM,
	B2_EM,
	B2_EM_DOM,
	ACCT_AGE,
	B2_ADD,
	B2_CITY,
	B2_DISTRICT,
	B2_STATE,
	B2_ZIP,
	B2_PH,
	A1_NM,
	A1_EM,
	A1_EM_DOM,
	A1_ADD,
	A1_CITY,
	A1_DISTRICT,
	A1_STATE,
	A1_ZIP,
	APPLE_MAPS_ADD,
	A1_PHONE,
	LATEST_A1_NM,
	LATEST_A1_EM,
	LASTEST_A1_EM_DOM,
	LATEST_A1_ADD,
	LATEST_A1_STATE,
	LATEST_A1_ZIP,
	LATEST_BOPIS3_NM,
	LATEST_BOPIS3_EM,
	PROD_DESC,
	TOTAL_VALUE,
	IP_ADD,
	IP_CLASS,
	IP_COMPANY,
	IP_COUNTRY,
	IP_BIN_COUNTRY,
	PAYMENT_TYPES,
	CARD_BIN,
	BIN_COUNTRY,
	CARD_HASH,
	CARD_ISSUER,
	GUID,
	SESSION_COOKIE
) as 
SELECT * FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame
WHERE 1=1
    AND SALES_ORG = '5600'
    AND CARD_BIN = '370021'
    OR (CARD_BIN = '373778'
    AND B2_EM_DOM = 'gmail.com');

-- ------------------------------------------------------------------------------
-- Pattern:    TRADER_5600_BIZ_GOV_457079
-- Type:       TH (Trader Hunting Patterns)
-- Activity:   Active
-- Created by: Stefanie Brabec
-- Created:    2025-09-24 18:10:55.060000
-- Columns (53): ORDER_ID, WEB_ORDER_ID, EVENT_TS, COMMIT_CD, COMMIT_DT, SALES_ORG, PO_TYPE, SALES_DISTRICT, DISCOUNT_STORE_TYPE, SLA, EXCEPTION_CD, B2_NM, B2_EM, B2_EM_DOM, ACCT_AGE, B2_ADD, B2_CITY, B2_DISTRICT, B2_STATE, B2_ZIP, B2_PH, A1_NM, A1_EM, A1_EM_DOM, A1_ADD, A1_CITY, A1_DISTRICT, A1_STATE, A1_ZIP, APPLE_MAPS_ADD, A1_PHONE, LATEST_A1_NM, LATEST_A1_EM, LASTEST_A1_EM_DOM, LATEST_A1_ADD, LATEST_A1_STATE, LATEST_A1_ZIP, LATEST_BOPIS3_NM, LATEST_BOPIS3_EM, PROD_DESC, TOTAL_VALUE, IP_ADD, IP_CLASS, IP_COMPANY, IP_COUNTRY, IP_BIN_COUNTRY, PAYMENT_TYPES, CARD_BIN, BIN_COUNTRY, CARD_HASH, CARD_ISSUER, GUID, SESSION_COOKIE
-- ------------------------------------------------------------------------------
create or replace view TRADER_5600_BIZ_GOV_457079(
	ORDER_ID,
	WEB_ORDER_ID,
	EVENT_TS,
	COMMIT_CD,
	COMMIT_DT,
	SALES_ORG,
	PO_TYPE,
	SALES_DISTRICT,
	DISCOUNT_STORE_TYPE,
	SLA,
	EXCEPTION_CD,
	B2_NM,
	B2_EM,
	B2_EM_DOM,
	ACCT_AGE,
	B2_ADD,
	B2_CITY,
	B2_DISTRICT,
	B2_STATE,
	B2_ZIP,
	B2_PH,
	A1_NM,
	A1_EM,
	A1_EM_DOM,
	A1_ADD,
	A1_CITY,
	A1_DISTRICT,
	A1_STATE,
	A1_ZIP,
	APPLE_MAPS_ADD,
	A1_PHONE,
	LATEST_A1_NM,
	LATEST_A1_EM,
	LASTEST_A1_EM_DOM,
	LATEST_A1_ADD,
	LATEST_A1_STATE,
	LATEST_A1_ZIP,
	LATEST_BOPIS3_NM,
	LATEST_BOPIS3_EM,
	PROD_DESC,
	TOTAL_VALUE,
	IP_ADD,
	IP_CLASS,
	IP_COMPANY,
	IP_COUNTRY,
	IP_BIN_COUNTRY,
	PAYMENT_TYPES,
	CARD_BIN,
	BIN_COUNTRY,
	CARD_HASH,
	CARD_ISSUER,
	GUID,
	SESSION_COOKIE
) as 
SELECT * FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame
WHERE 1=1
AND SALES_ORG = '5600'
AND DISCOUNT_STORE_TYPE = 'BIZ_OR_GOV'
and CARD_BIN = '457079'
and IP_COMPANY = 'verizon'
and B2_EM_DOM like any ('gmail.com','yahoo.com','hotmail.com')
and LATEST_A1_STATE in ('MA');

-- ------------------------------------------------------------------------------
-- Pattern:    TRADER_AMR_Common_EM_DOM
-- Type:       TH (Trader Hunting Patterns)
-- Activity:   Active
-- Created by: Greg Mills
-- Created:    2025-08-29 23:22:05.973000
-- Columns (7): ORDER_ID, WEB_ORDER_ID, CLEANED_NAME, EMAIL_HANDLE, NM2EM_MATCH_SCORE, HVP, SIXTY_DAY_LIMIT_IND
-- ------------------------------------------------------------------------------
create or replace view TRADER_AMR_COMMON_EM_DOM(
	ORDER_ID,
	WEB_ORDER_ID,
	CLEANED_NAME,
	EMAIL_HANDLE,
	NM2EM_MATCH_SCORE,
	HVP,
	SIXTY_DAY_LIMIT_IND
) as
WITH trader_frame_filtered AS (
    SELECT ORDER_ID, WEB_ORDER_ID, B2_NM, B2_EM, APPLE_MAPS_ADD
    FROM gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_traderhunt_frame
    WHERE SALES_ORG IN ('3400', '5600')
    AND SALES_DISTRICT = 'IN02'
    AND EVENT_TS >= current_date - 60
),
apid_scriptor_base AS (
    SELECT
        tf.ORDER_ID,
        tf.WEB_ORDER_ID,
        LOWER(REPLACE(tf.b2_nm, ' ', '')) AS cleaned_name,
        LOWER(SPLIT_PART(tf.b2_em, '@', 1)) AS email_handle,
        CASE
            WHEN LENGTH(REPLACE(tf.b2_nm, ' ', '')) = 0 OR tf.b2_em IS NULL THEN NULL
            ELSE JAROWINKLER_SIMILARITY(
                LOWER(REPLACE(tf.b2_nm, ' ', '')),
                LOWER(SPLIT_PART(tf.b2_em, '@', 1))
            )
        END AS nm2em_match_score,
        COUNT(*) OVER (PARTITION BY tf.APPLE_MAPS_ADD) AS HVP
    FROM trader_frame_filtered tf
    JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_review_frame_vw rv
        ON tf.ORDER_ID = rv.ORDER_ID
    WHERE rv.B2_EM_DOM IN ('163.com', 'aol.com', 'gmail.com', 'hotmail.com', 'icloud.com', 'mac.com', 'me.com', 'outlook.com', 'qq.com', 'yahoo.com')
    AND (rv.PAYMENT_TYPE LIKE '%APID%' OR rv.PAYMENT_TYPE LIKE '%AGCD%')
),
apid_scriptor_th AS (
    SELECT *,
        CASE
            WHEN HVP > 6 THEN 'EXCEEDS'
            WHEN HVP <= 6 THEN 'UNDER LIMIT'
        END AS SIXTY_DAY_LIMIT_IND
    FROM apid_scriptor_base
    WHERE nm2em_match_score < 0.80
)
SELECT * FROM apid_scriptor_th;

-- ------------------------------------------------------------------------------
-- Pattern:    TRADER_AMR_UNIQUE_EM_DOM
-- Type:       TH (Trader Hunting Patterns)
-- Activity:   Active
-- Created by: Greg Mills
-- Created:    2025-08-29 23:22:10.071000
-- Columns (7): ORDER_ID, WEB_ORDER_ID, CLEANED_NAME, EMAIL_HANDLE, NM2EM_MATCH_SCORE, HVP, SIXTY_DAY_LIMIT_IND
-- ------------------------------------------------------------------------------
create or replace view TRADER_AMR_UNIQUE_EM_DOM(
	ORDER_ID,
	WEB_ORDER_ID,
	CLEANED_NAME,
	EMAIL_HANDLE,
	NM2EM_MATCH_SCORE,
	HVP,
	SIXTY_DAY_LIMIT_IND
) as 
WITH trader_frame_filtered AS (
    -- Filter trader frame first to reduce data volume
    SELECT ORDER_ID, WEB_ORDER_ID, B2_NM, B2_EM, APPLE_MAPS_ADD
    FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame
    WHERE SALES_ORG IN ('3400', '5600') 
    AND SALES_DISTRICT = 'IN02'
    AND EVENT_TS >= current_date - 60  -- Limit to last 60 days for HVP calculation
),
apid_scriptor_base AS (
    SELECT 
        tf.ORDER_ID,
        tf.WEB_ORDER_ID,
        LOWER(REPLACE(tf.b2_nm, ' ', '')) AS cleaned_name,
        LOWER(SPLIT_PART(tf.b2_em, '@', 1)) AS email_handle,
        CASE 
            WHEN LENGTH(REPLACE(tf.b2_nm, ' ', '')) = 0 OR tf.b2_em IS NULL THEN NULL 
            ELSE JAROWINKLER_SIMILARITY(
                LOWER(REPLACE(tf.b2_nm, ' ', '')), 
                LOWER(SPLIT_PART(tf.b2_em, '@', 1))
            ) 
        END AS nm2em_match_score,
        COUNT(*) OVER (PARTITION BY tf.APPLE_MAPS_ADD) AS HVP
    FROM trader_frame_filtered tf
    JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_review_frame_vw rv 
        ON tf.ORDER_ID = rv.ORDER_ID
    WHERE rv.B2_EM_DOM NOT IN ('163.com', 'aol.com', 'gmail.com', 'hotmail.com', 'icloud.com', 'mac.com', 'me.com', 'outlook.com', 'qq.com', 'yahoo.com')
    AND RIGHT(rv.B2_EM_DOM, 3) NOT IN ('.es', '.fr')
    AND (rv.PAYMENT_TYPE LIKE '%APID%' OR rv.PAYMENT_TYPE LIKE '%AGCD%')
),
apid_scriptor_th AS (
    SELECT *,
        CASE 
            WHEN HVP > 6 THEN 'EXCEEDS'
            WHEN HVP <= 6 THEN 'UNDER LIMIT'
        END AS SIXTY_DAY_LIMIT_IND
    FROM apid_scriptor_base
)
SELECT 
    ORDER_ID, 
    WEB_ORDER_ID, 
    cleaned_name, 
    email_handle, 
    nm2em_match_score, 
    HVP, 
    SIXTY_DAY_LIMIT_IND
FROM apid_scriptor_th;

-- ------------------------------------------------------------------------------
-- Pattern:    trader_5600_bizgov_bin_512068
-- Type:       TH (Trader Hunting Patterns)
-- Activity:   Active
-- Created by: System
-- Created:    2025-08-12 21:35:58.994000
-- Columns (52): ORDER_ID, WEB_ORDER_ID, EVENT_TS, COMMIT_CD, COMMIT_DT, SALES_ORG, PO_TYPE, SALES_DISTRICT, DISCOUNT_STORE_TYPE, SLA, EXCEPTION_CD, B2_NM, B2_EM, B2_EM_DOM, ACCT_AGE, B2_ADD, B2_CITY, B2_DISTRICT, B2_STATE, B2_ZIP, B2_PH, A1_NM, A1_EM, A1_EM_DOM, A1_ADD, A1_CITY, A1_DISTRICT, A1_STATE, A1_ZIP, A1_PHONE, LATEST_A1_NM, LATEST_A1_EM, LASTEST_A1_EM_DOM, LATEST_A1_ADD, LATEST_A1_STATE, LATEST_A1_ZIP, LATEST_BOPIS3_NM, LATEST_BOPIS3_EM, PROD_DESC, TOTAL_VALUE, IP_ADD, IP_CLASS, IP_COMPANY, IP_COUNTRY, IP_BIN_COUNTRY, PAYMENT_TYPES, CARD_BIN, BIN_COUNTRY, CARD_HASH, CARD_ISSUER, GUID, SESSION_COOKIE
-- ------------------------------------------------------------------------------
create or replace view TRADER_5600_BIZGOV_BIN_512068(
	ORDER_ID,
	WEB_ORDER_ID,
	EVENT_TS,
	COMMIT_CD,
	COMMIT_DT,
	SALES_ORG,
	PO_TYPE,
	SALES_DISTRICT,
	DISCOUNT_STORE_TYPE,
	SLA,
	EXCEPTION_CD,
	B2_NM,
	B2_EM,
	B2_EM_DOM,
	ACCT_AGE,
	B2_ADD,
	B2_CITY,
	B2_DISTRICT,
	B2_STATE,
	B2_ZIP,
	B2_PH,
	A1_NM,
	A1_EM,
	A1_EM_DOM,
	A1_ADD,
	A1_CITY,
	A1_DISTRICT,
	A1_STATE,
	A1_ZIP,
	A1_PHONE,
	LATEST_A1_NM,
	LATEST_A1_EM,
	LASTEST_A1_EM_DOM,
	LATEST_A1_ADD,
	LATEST_A1_STATE,
	LATEST_A1_ZIP,
	LATEST_BOPIS3_NM,
	LATEST_BOPIS3_EM,
	PROD_DESC,
	TOTAL_VALUE,
	IP_ADD,
	IP_CLASS,
	IP_COMPANY,
	IP_COUNTRY,
	IP_BIN_COUNTRY,
	PAYMENT_TYPES,
	CARD_BIN,
	BIN_COUNTRY,
	CARD_HASH,
	CARD_ISSUER,
	GUID,
	SESSION_COOKIE
) as (
    -- CT rate('2025-07-29' - 10days): 1.00 44 CT/44
    select * from gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame
    where 1=1
    and EVENT_TS >= current_date - 5
    and SALES_ORG ='5600'
    and DISCOUNT_STORE_TYPE = 'biz_or_gov'
    and CARD_BIN in ('512068')
    and B2_EM_DOM in ('gmail.com')
    and LATEST_A1_STATE in ('LA')
    and PROD_DESC like ('%IPHONE 16%')
    and PROD_DESC like ('%CASE%')
);

-- ------------------------------------------------------------------------------
-- Pattern:    trader_apid_scriptor_low_match
-- Type:       TH (Trader Hunting Patterns)
-- Activity:   Active
-- Created by: System
-- Created:    2025-09-09 23:45:03.463000
-- Columns (7): ORDER_ID, WEB_ORDER_ID, CLEANED_NAME, EMAIL_HANDLE, NM2EM_MATCH_SCORE, HVP, SIXTY_DAY_LIMIT_IND
-- ------------------------------------------------------------------------------
create or replace view TRADER_APID_SCRIPTOR_LOW_MATCH(
	ORDER_ID,
	WEB_ORDER_ID,
	CLEANED_NAME,
	EMAIL_HANDLE,
	NM2EM_MATCH_SCORE,
	HVP,
	SIXTY_DAY_LIMIT_IND
) as
WITH apid_scriptor_base AS (
    SELECT
        thf.ORDER_ID,
        thf.WEB_ORDER_ID,
        LOWER(REPLACE(b2_nm, ' ', '')) AS cleaned_name,
        LOWER(SPLIT_PART(b2_em, '@', 1)) AS email_handle,
        SPLIT_PART(b2_em, '@', 2) AS b2_em_dom,
        CASE WHEN B2_EM_DOM IN ('163.com', 'aol.com', 'gmail.com', 'hotmail.com', 'icloud.com',
                                'mac.com', 'me.com', 'outlook.com', 'qq.com', 'yahoo.com')
             THEN 1
             ELSE 0
        END as Common_em_dom_ind,
        CASE
            WHEN LENGTH(REPLACE(b2_nm, ' ', '')) = 0 OR b2_em IS NULL THEN NULL
            ELSE JAROWINKLER_SIMILARITY(
                LOWER(REPLACE(b2_nm, ' ', '')),
                LOWER(SPLIT_PART(b2_em, '@', 1))
            )
        END AS nm2em_match_score,
        a1v.APPLE_map_ADD,
        a1v.latest_a1_add,
        a1v.Apple_map_HVP,
        a1v.latest_a1_HVP,
        a1v.Exceed_ind_60d as SIXTY_DAY_LIMIT_IND
    FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame thf
    JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_shipto_add_velocity_vw a1v
        ON thf.web_order_id = a1v.web_order_id
    WHERE thf.SALES_ORG IN ('3400', '5600')
      AND SALES_DISTRICT = 'IN02'
      AND PAYMENT_TYPES LIKE ANY ('AGCD%', 'APID%')
)
SELECT
    ORDER_ID,
    WEB_ORDER_ID,
    cleaned_name,
    email_handle,
    nm2em_match_score,
    APPLE_map_HVP as HVP,
    SIXTY_DAY_LIMIT_IND
FROM apid_scriptor_base
WHERE nm2em_match_score < 0.80;

-- ================================================================================
-- SECTION: GCT — Good Customer Patterns  (3 patterns)
-- ================================================================================

-- ------------------------------------------------------------------------------
-- Pattern:    TRADER_FREIGHT_FORWARDER
-- Type:       GCT (Good Customer Patterns)
-- Activity:   Active
-- Created by: Stefanie Brabec
-- Created:    2025-09-14 23:01:50.742000
-- Columns (55): ORDER_ID, WEB_ORDER_ID, EVENT_TS, COMMIT_CD, COMMIT_DT, SALES_ORG, PO_TYPE, SALES_DISTRICT, DISCOUNT_STORE_TYPE, SLA, EXCEPTION_CD, B2_NM, B2_EM, B2_EM_DOM, ACCT_AGE, B2_ADD, B2_CITY, B2_DISTRICT, B2_STATE, B2_ZIP, B2_PH, A1_NM, A1_EM, A1_EM_DOM, A1_ADD, A1_CITY, A1_DISTRICT, A1_STATE, A1_ZIP, APPLE_MAPS_ADD, A1_PHONE, LATEST_A1_NM, LATEST_A1_EM, LATEST_A1_EM_DOM, LATEST_A1_ADD, LATEST_A1_STATE, LATEST_A1_ZIP, LATEST_BOPIS3_NM, LATEST_BOPIS3_EM, PROD_DESC, TOTAL_VALUE, IP_ADD, IP_CLASS, IP_COMPANY, IP_COUNTRY, IP_BIN_COUNTRY, PAYMENT_TYPES, CARD_BIN, BIN_COUNTRY, CARD_HASH, CARD_ISSUER, GUID, SESSION_COOKIE, OUTPUT_APPLE_MAPS_NORMALIZED_ADDRESS_UNIT_STRIPPED, TTL_ORDERS
-- ------------------------------------------------------------------------------
create or replace view TRADER_FREIGHT_FORWARDER(
	ORDER_ID,
	WEB_ORDER_ID,
	EVENT_TS,
	COMMIT_CD,
	COMMIT_DT,
	SALES_ORG,
	PO_TYPE,
	SALES_DISTRICT,
	DISCOUNT_STORE_TYPE,
	SLA,
	EXCEPTION_CD,
	B2_NM,
	B2_EM,
	B2_EM_DOM,
	ACCT_AGE,
	B2_ADD,
	B2_CITY,
	B2_DISTRICT,
	B2_STATE,
	B2_ZIP,
	B2_PH,
	A1_NM,
	A1_EM,
	A1_EM_DOM,
	A1_ADD,
	A1_CITY,
	A1_DISTRICT,
	A1_STATE,
	A1_ZIP,
	APPLE_MAPS_ADD,
	A1_PHONE,
	LATEST_A1_NM,
	LATEST_A1_EM,
	LATEST_A1_EM_DOM,
	LATEST_A1_ADD,
	LATEST_A1_STATE,
	LATEST_A1_ZIP,
	LATEST_BOPIS3_NM,
	LATEST_BOPIS3_EM,
	PROD_DESC,
	TOTAL_VALUE,
	IP_ADD,
	IP_CLASS,
	IP_COMPANY,
	IP_COUNTRY,
	IP_BIN_COUNTRY,
	PAYMENT_TYPES,
	CARD_BIN,
	BIN_COUNTRY,
	CARD_HASH,
	CARD_ISSUER,
	GUID,
	SESSION_COOKIE,
	OUTPUT_APPLE_MAPS_NORMALIZED_ADDRESS_UNIT_STRIPPED,
	TTL_ORDERS
) as (
select * from GBI_FRAUD_BAP_DB.AI_LIVE_BIZ_APP.TRADER_TFA_TRADERHUNT_FRAME ttf
left join gbi_fraud_bap_db.ai_live_biz_app.tfa_aso_freight_forwarder_address_log ffa
        on ttf.APPLE_MAPS_ADD = ffa.OUTPUT_APPLE_MAPS_NORMALIZED_ADDRESS_UNIT_STRIPPED
    where 1=1
    and EXCEPTION_CD = 'D68'
    and ttf.APPLE_MAPS_ADD is not null
    and ffa.TTL_ORDERS >= 3
    );

-- ------------------------------------------------------------------------------
-- Pattern:    trader_genuine_customer_orders
-- Type:       GCT (Good Customer Patterns)
-- Activity:   Active
-- Created by: System
-- Created:    2025-08-12 21:40:00.402000
-- Columns (5): WEB_ORDER_ID, ORDER_ID, GENUINE_CUSTOMER_IND, SCORE_THRESHOLD_DIFFERENCE, ACTION_LEVEL
-- ------------------------------------------------------------------------------
create or replace view TRADER_GENUINE_CUSTOMER_ORDERS(
	WEB_ORDER_ID,
	ORDER_ID,
	GENUINE_CUSTOMER_IND,
	SCORE_THRESHOLD_DIFFERENCE,
	ACTION_LEVEL
) as (
    --Use Action Notes "SDS TRADER CLEAR GCT"
    --This query looks for orders with genuine customer behavior under the following criteria
    --CTO / Personalize / Apple Care + / Good payment types / Sales District / Engraving

    --NPI back tested '2024-09-13' to '2024-12-31'
    --Action Level Auto Release: Genuine Customer Rate: 97.4% Genuine Label Rate: 94.3%
    --Action Level BAU Release: Genuine Customer Rate: 96.5% Genuine Label Rate: 93.2%
    --Action Level Hot Release: Genuine Customer Rate: 96.3% Genuine Label Rate: 92.8%

    SELECT tb.web_order_id                              AS web_order_id
         , tb.order_id                                  AS order_id
         --Genuine customer indicators
         , CASE WHEN taws.data_items_id LIKE '%"Z%'
                --remove Sales Orgs with lower Genuine Customer/Label Rates
                 AND taws.data_salesorg NOT IN (1200,2000,5600,8200,8400)
                THEN 'CTO'
                WHEN taws.data_items_id LIKE '%"P%'
                --remove Sales Orgs/PO type with lower Genuine Customer/Label Rates
                 AND NOT (taws.data_salesorg IN (2000,8400) OR taws.data_potype = 'GWEB')
                THEN 'PERSONALIZE'
                WHEN taws.data_items_id LIKE '%"S%'
                --remove Sales Orgs/PO types with lower Genuine Customer/Label Rates
                 AND NOT (taws.data_salesorg IN (2000,3400,5600,8200)
                      OR (taws.data_salesorg = 8400 AND data_potype IN ('GWEB','GWBM')))
                THEN 'AC+'
                WHEN taws.data_paymentterms IN ('Z105','ZPRE','ZJ28','ZJ61')
                --remove PO type with lower Genuine Customer/Label Rates
                 AND taws.data_potype NOT IN ('GWEB','GWBM')
                THEN 'GENUINE PAYMENT TYPES'
                --remove Sales District with lower Genuine Customer/Label Rates
                WHEN taws.data_salesdistrict in ('IN01','RW01','IN26','RW26','RB01')  AND taws.data_salesorg != 0200
                THEN 'GENUINE SALES DISTRICT'
                WHEN taws.data_items_updatedengravingtext1 IS NOT NULL
                --remove Sales Orgs/PO type with lower Genuine Customer/Label Rates
                 AND taws.data_salesorg != 1200
                 AND taws.data_potype NOT IN ('GWEB','GWBM','MOBW')
                THEN 'ENGRAVING'
                 END                                     AS genuine_customer_ind
         --Get threshold distance to filter
         , po.score_threshold_difference
         --Action level conditions. Mainly use for setting action priority
         , CASE WHEN po.score_threshold_difference <= 0.1 THEN 'Auto Release'
                WHEN po.score_threshold_difference <= 0.6 THEN 'BAU Release'
                ELSE 'Hot Release'
                 END                                     AS action_level
      FROM gbi_fraud_bap_db.ai_live_biz_app.ww_trader_backlog tb
      JOIN (SELECT *
            FROM gbi_fraud_bap_db.ai_live_biz_app.tfa_aos_wo_slice
            WHERE 1=1
             AND event_dt >= CURRENT_DATE - 10
             AND output_order_type NOT IN ('adp','egc')   --exclude digital orders
             AND output_digital_egc_model_eligible = 'false') taws
        ON  tb.web_order_id = taws.data_weborder
      JOIN (SELECT checkoutsessionid
                 , CAST(model_threshold AS FLOAT)     AS threshold
                 , CAST(trader_model_score AS FLOAT)  AS model_score
                 , ROUND(model_score - threshold, 3)     AS score_threshold_difference
              FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_place_order
             WHERE event_dt >= current_date - 10
           QUALIFY ROW_NUMBER() OVER (PARTITION BY checkoutsessionid ORDER BY event_ts DESC NULLS LAST) = 1) po
        ON taws.output_checkoutsessionid = po.checkoutsessionid
     WHERE genuine_customer_ind IS NOT NULL
);

-- ------------------------------------------------------------------------------
-- Pattern:    trader_good_product_set
-- Type:       GCT (Good Customer Patterns)
-- Activity:   Active
-- Created by: System
-- Created:    2025-08-12 21:40:00.402000
-- Columns (6): WEB_ORDER_ID, ORDER_ID, SALES_ORG_CD, ITEMS_COUNT, SCORE_THRESHOLD_DIFFERENCE, ACTION_LEVEL
-- ------------------------------------------------------------------------------
create or replace view TRADER_GOOD_PRODUCT_SET(
	WEB_ORDER_ID,
	ORDER_ID,
	SALES_ORG_CD,
	ITEMS_COUNT,
	SCORE_THRESHOLD_DIFFERENCE,
	ACTION_LEVEL
) as (
    --Use Action Notes "SDS TRADER CLEAR GPS"
    --Query to get genuine non-trader orders with a good product set
    --Good product set is defined to have at least 1 hero product and a greater number of accessories+Apple Care +

    --NPI back tested '2024-09-13' to '2024-12-31'
    --Action Level Auto Release: Genuine Customer Rate: 98.4% Genuine Label Rate: 95.1%
    --Action Level BAU Release: Genuine Customer Rate: 97.9% Genuine Label Rate: 92%
    --Action Level Hot Release: Genuine Customer Rate: 97.7% Genuine Label Rate: 90.2%

    WITH categories AS (
        SELECT tb.web_order_id                               AS web_order_id
             , tb.order_id                                   AS order_id
             , tb.sales_org_cd                               AS sales_org_cd
        --Get threshold distance to filter
             , po.score_threshold_difference
        --Action level conditions. Mainly use for setting action priority
             , CASE WHEN po.score_threshold_difference <= 0.3
                     AND data_potype NOT IN ('GWEB','GWBM')
                     AND sales_org_cd != 2000
                    THEN 'Auto Release'
                    WHEN data_potype != 'GWEB'
                    THEN 'BAU Release'
                    ELSE 'Hot Release'
                     END                                     AS action_level
        --categorize item under Hero product, Apple Care +, Accessory
             , CASE WHEN ph.prod_class_id LIKE ANY ('1%' --Mac
                                                  , '2%' --iPod
                                                  , '3P' --iPhone
                                                  , '3C' --Watch
                                                  , '5%' --Display
                                                  , '8A') --iPad
                    THEN 'hero'
                    WHEN ph.prod_class_id = 'S'
                     AND ph.prod_family_desc LIKE 'AC+%'
                    THEN 'ac+'
                    WHEN ph.prod_class_id LIKE ('6%')
                    THEN 'acc'
                     END                                     AS category
             , SUM(soir.order_qty)                           AS category_count
        --trader backlog(all ST orders)
          FROM gbi_fraud_bap_db.ai_live_biz_app.ww_trader_backlog tb
        --join sales_order_item_reg to get item quantity per line item
          JOIN gbi_fraud_semantic_db.ai.sales_order_item_reg soir
            ON tb.order_id = soir.order_id
        --join product_hierarchy table for product class id
          JOIN (SELECT DISTINCT prod_class_id, prod_id, prod_family_desc
                  FROM gbi_fraud_semantic_db.ai.product_hierarchy) ph
            ON soir.prod_id = ph.prod_id
          JOIN (SELECT data_weborder, output_checkoutsessionid, data_potype
                  FROM gbi_fraud_bap_db.ai_live_biz_app.tfa_aos_wo_slice
                 WHERE event_dt >= current_date - 10) taws
            ON tb.web_order_id = taws.data_weborder
          JOIN (SELECT checkoutsessionid
                     , CAST(po.model_threshold AS FLOAT)     AS threshold
                     , CAST(po.trader_model_score AS FLOAT)  AS model_score
                     , ROUND(model_score - threshold, 3)     AS score_threshold_difference
                  FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_place_order po
                 WHERE event_dt >= current_date - 10
               QUALIFY ROW_NUMBER() OVER (PARTITION BY checkoutsessionid ORDER BY event_ts DESC NULLS LAST) = 1) po
            ON taws.output_checkoutsessionid = po.checkoutsessionid
         WHERE 1 = 1
        --remove Vision and related accessories, Vision accessories are always paired to the hero product
           AND ph.prod_class_id NOT IN ('3Q', '6K')
         GROUP BY 1, 2, 3, 4, 5, 6)
    --creates aggregation of item count by category created in the categories table
    , agg_table AS (
        SELECT web_order_id
             , order_id
             , sales_org_cd
             , score_threshold_difference
             , action_level
             , OBJECT_AGG(category, category_count) AS items_count
          FROM categories
         GROUP BY 1, 2, 3, 4, 5)
    --main query
        SELECT web_order_id
             , order_id
             , sales_org_cd
             , items_count
             , score_threshold_difference
             , action_level
          FROM agg_table a
         WHERE 1 = 1
            --gets orders that have more acc/ac+ than hero products, change conditions for 8200
           AND ((a.sales_org_cd != 8200 AND items_count:"ac+"::NUMBER + items_count:"acc"::NUMBER > items_count:"hero"::NUMBER)
            OR   (a.sales_org_cd = 8200 AND items_count:"acc"::NUMBER > items_count:"hero"::NUMBER))
            --remove orders with no hero products
           AND items_count:"hero"::NUMBER > 0
            --remove orders with more than 10 accessories
           AND items_count:"acc"::NUMBER < 10
);

