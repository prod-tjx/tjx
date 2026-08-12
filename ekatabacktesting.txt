USE ROLE GBI_FRAUD_SDS_ANALYTICS_CUST_MAIN_ROLE;

-- 1) Materialize card-hash fraud rates
CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app.trevora_cc_fr2 AS
WITH
  filtered_base AS (
    SELECT
      data_weborder,
      event_ts,
      data_payment_cardnumberhash[0] AS card_hash
    FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order
    WHERE
      event_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
      AND output_digital_egc_model_eligible = 'false'
      AND data_salesorg     IN ('5600','3400')
      AND output_order_type IN ('idl','sds','other')
      AND data_payment_cardnumberhash[0] IS NOT NULL
  ),
  latest_cc AS (
    SELECT
      data_weborder,
      event_ts        AS latest_ts,
      card_hash
    FROM filtered_base
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY data_weborder
      ORDER BY event_ts DESC
    ) = 1
  ),
  hist_cc AS (
    SELECT
      fb.data_weborder,
      fb.card_hash,
      CASE WHEN fo.etl_create_ts IS NOT NULL THEN fo.etl_create_ts ELSE fb.event_ts END AS ts_for_join,
      fb.event_ts   AS kafka_ts,
      fo.etl_create_ts AS fo_ts
    FROM filtered_base AS fb
    LEFT JOIN gbi_fraud_semantic_db.ai.fraud_order fo
      ON fo.web_order_id = fb.data_weborder
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY fb.data_weborder, fb.card_hash
      ORDER BY fb.event_ts DESC
    ) = 1
  )
SELECT
  l.data_weborder,
  l.card_hash,
  COUNT( CASE WHEN h.ts_for_join < l.latest_ts THEN 1 END )    AS ttl_prior_orders,
  COUNT( CASE WHEN h.fo_ts       < l.latest_ts THEN 1 END )    AS ttl_prior_fraud,
  CASE
    WHEN COUNT( CASE WHEN h.ts_for_join < l.latest_ts THEN 1 END ) > 0
    THEN COUNT( CASE WHEN h.fo_ts < l.latest_ts THEN 1 END ) * 100.00
         / COUNT( CASE WHEN h.ts_for_join < l.latest_ts THEN 1 END )
    ELSE NULL
  END                                                           AS cc_fraud_rate
FROM latest_cc l
LEFT JOIN hist_cc h
  ON h.card_hash    = l.card_hash
 AND h.ts_for_join  < l.latest_ts
GROUP BY l.data_weborder, l.card_hash
;

-- 2) Materialize card-bin fraud rates
CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app.trevora_bin_fr2 AS

WITH base_bin AS (
  SELECT
    k.data_weborder,
    k.event_ts,
    CASE
      WHEN k.data_splittenderflag = 'X'
           AND ARRAY_CONTAINS('APID'::VARIANT, k.data_payment_actualsapcardtype) THEN 'APID_split_payment'
      WHEN k.data_splittenderflag IS NULL
           AND ARRAY_CONTAINS('APID'::VARIANT, k.data_payment_actualsapcardtype) THEN 'APID_solo'
      WHEN k.data_splittenderflag = 'X'
           AND ARRAY_TO_STRING(k.data_payment_actualsapcardtype::ARRAY, ' ')
               LIKE ANY('%AGCD%','%CCGC%') THEN 'AGCD_split_payment'
      WHEN k.data_splittenderflag IS NULL
           AND ARRAY_TO_STRING(k.data_payment_actualsapcardtype::ARRAY, ' ')
               LIKE ANY('%AGCD%','%CCGC%') THEN 'AGCD_solo'
      WHEN k.data_splittenderflag = 'X'
           AND ARRAY_TO_STRING(k.data_payment_cardbin::ARRAY, ' ')
               LIKE '%O-%' THEN 'PayPal_split_payment'
      WHEN k.data_splittenderflag IS NULL
           AND ARRAY_TO_STRING(k.data_payment_cardbin::ARRAY, ' ')
               LIKE '%O-%' THEN 'PayPal_solo'
      WHEN k.data_splittenderflag = 'X'
        THEN CONCAT(k.data_payment_cardbin[0], '_split')
      ELSE
        k.data_payment_cardbin[0]
    END AS improved_cardbin
  FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order k
  WHERE
    k.event_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
    AND k.output_digital_egc_model_eligible = 'false'
    AND k.data_salesorg     IN ('5600','3400')
    AND k.output_order_type IN ('idl','sds','other')
    AND k.data_payment_cardbin IS NOT NULL
    AND k.data_payment_cardbin[0] <> '111111'
),

annotated AS (
  SELECT
    bb.data_weborder,
    bb.event_ts,
    bb.improved_cardbin,
    -- all prior orders for this bin
    COUNT(*)  OVER (
      PARTITION BY bb.improved_cardbin
      ORDER BY bb.event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS ttl_prior_orders,
    -- all prior fraud flags for this bin
    COUNT(fo.web_order_id)  OVER (
      PARTITION BY bb.improved_cardbin
      ORDER BY bb.event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS ttl_prior_fraud
  FROM
    base_bin bb
  LEFT JOIN
    gbi_fraud_semantic_db.ai.fraud_order fo
      ON fo.web_order_id = bb.data_weborder
),

latest_per_order AS (
  SELECT
    data_weborder,
    ttl_prior_orders,
    ttl_prior_fraud,
    event_ts,
    -- compute rate or sentinel
    CASE
      WHEN ttl_prior_orders > 0
      THEN ttl_prior_fraud * 100.0 / ttl_prior_orders
      ELSE NULL
    END AS bin_fraud_rate,
    ROW_NUMBER() OVER (
      PARTITION BY data_weborder
      ORDER BY event_ts DESC
    ) AS rn
  FROM
    annotated
)

SELECT
  data_weborder,
  ttl_prior_orders,
  ttl_prior_fraud,
  bin_fraud_rate
FROM
  latest_per_order
WHERE
  rn = 1;


CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app.trevora_b2em_fr2 AS

SELECT
  latest.data_weborder,
  -- how many prior orders this email had
  COUNT(CASE WHEN hist.kafka_ts < latest.event_ts THEN 1 END) AS ttl_prior_orders,
  -- how many of those prior orders were marked fraud
  COUNT(CASE WHEN hist.fo_ts   < latest.event_ts THEN 1 END) AS ttl_prior_fraud,
  -- fraud rate = prior frauds / prior orders
  CASE
    WHEN COUNT(CASE WHEN hist.kafka_ts < latest.event_ts THEN 1 END) > 0
    THEN COUNT(CASE WHEN hist.fo_ts   < latest.event_ts THEN 1 END) * 100.0
         / COUNT(CASE WHEN hist.kafka_ts < latest.event_ts THEN 1 END)
    ELSE NULL
  END AS b2em_fraud_rate
FROM (
  -- 1) pick the single latest record per order (the “current” email event)
  SELECT
    data_weborder,
    event_ts,
    data_billtoemail AS email_dim
  FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order
  WHERE
    event_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
    AND data_billtoemail IS NOT NULL
    AND data_billtoemail NOT IN (
      'appleapac@globalrewardsolutions.com',
      'noreply@rock.apple.com',
      '486487291@qq.com',
      'Student1777@fullmasai.jp',
      '444211876@qq.com'
    )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY data_weborder
    ORDER BY event_ts DESC NULLS LAST
  ) = 1
) AS latest
LEFT JOIN (
  -- 2) pick the latest historical record for each order
  SELECT
    k.data_weborder,
    CASE WHEN fo.etl_create_ts IS NOT NULL THEN fo.etl_create_ts ELSE k.event_ts END AS ts_for_join,
    k.event_ts                         AS kafka_ts,
    fo.etl_create_ts                   AS fo_ts,
    k.data_billtoemail                 AS email_dim
  FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order k
  LEFT JOIN gbi_fraud_semantic_db.ai.fraud_order fo
    ON fo.web_order_id = k.data_weborder
  WHERE
    k.event_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
    AND k.data_billtoemail IS NOT NULL
    AND k.data_billtoemail NOT IN (
      'appleapac@globalrewardsolutions.com',
      'noreply@rock.apple.com',
      '486487291@qq.com',
      'Student1777@fullmasai.jp',
      '444211876@qq.com'
    )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY k.data_weborder
    ORDER BY k.event_ts DESC NULLS LAST
  ) = 1
) AS hist
  ON hist.email_dim   = latest.email_dim
 AND hist.ts_for_join < latest.event_ts    -- only prior events
GROUP BY latest.data_weborder
;



CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app.trevora_a1em_fr2 AS
WITH a1em_fr2 AS (
    SELECT
        latest.data_weborder,
        COUNT(CASE WHEN latest.event_ts > hist.kafka_ts THEN 1 END) AS ttl_prior_orders,
        COUNT(CASE WHEN latest.event_ts > hist.fo_ts    THEN 1 END) AS ttl_prior_fraud,
        CASE
            WHEN COUNT(CASE WHEN latest.event_ts > hist.kafka_ts THEN 1 END) > 0
            THEN COUNT(CASE WHEN latest.event_ts > hist.fo_ts THEN 1 END) * 100.0
                 / COUNT(CASE WHEN latest.event_ts > hist.kafka_ts THEN 1 END)
            ELSE NULL
        END AS a1em_fraud_rate
    FROM (
        -- 1) latest ship-to email per order
        SELECT
            data_weborder,
            event_ts,
            data_shiptoemail AS email_dim
        FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order
        WHERE
            event_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
            AND data_shiptoemail IS NOT NULL
            AND data_shiptoemail NOT IN (
                'appleapac@globalrewardsolutions.com',
                'noreply@rock.apple.com',
                '486487291@qq.com',
                'Student1777@fullmasai.jp',
                '444211876@qq.com',
                '418612210@qq.com'
            )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY data_weborder
            ORDER BY event_ts DESC NULLS LAST
        ) = 1
    ) latest
    LEFT JOIN (
        -- 2) latest historical record per order (for the same email)
        SELECT
            k.data_weborder,
            CASE
                WHEN fo.etl_create_ts IS NOT NULL THEN fo.etl_create_ts
                ELSE k.event_ts
            END                       AS ts_for_join,
            k.event_ts                 AS kafka_ts,
            fo.etl_create_ts           AS fo_ts,
            k.data_shiptoemail         AS email_dim
        FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order k
        LEFT JOIN gbi_fraud_semantic_db.ai.fraud_order fo
          ON fo.web_order_id = k.data_weborder
        WHERE
            k.event_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
            AND k.data_shiptoemail IS NOT NULL
            AND k.data_shiptoemail NOT IN (
                'appleapac@globalrewardsolutions.com',
                'noreply@rock.apple.com',
                '486487291@qq.com',
                'Student1777@fullmasai.jp',
                '444211876@qq.com',
                '418612210@qq.com'
            )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY k.data_weborder
            ORDER BY k.event_ts DESC NULLS LAST
        ) = 1
    ) hist
      ON hist.email_dim    = latest.email_dim
     AND hist.ts_for_join < latest.event_ts
    GROUP BY latest.data_weborder
)
SELECT * FROM a1em_fr2;


-- SELECT * FROM cc_fr2
-- LIMIT 12;
--Select card_hash2 from cc_fr2
--W1364940235, W1318622097,
-- Not fraudy W1348926700, W1358883163


CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app.trevora_cookie_fr2 AS
WITH cookie_fr2 AS (
    SELECT
        latest.data_weborder,
        -- how many prior orders this cookie has
        COUNT(CASE WHEN latest.event_ts > hist.kafka_ts THEN 1 END)      AS ttl_prior_orders,
        -- how many of those were fraud
        COUNT(CASE WHEN latest.event_ts > hist.fo_ts    THEN 1 END)      AS ttl_prior_fraud,
        -- fraud rate = prior frauds / prior orders, else -99
        CASE
            WHEN COUNT(CASE WHEN latest.event_ts > hist.kafka_ts THEN 1 END) > 0
            THEN COUNT(CASE WHEN latest.event_ts > hist.fo_ts THEN 1 END) * 100.0
                 / COUNT(CASE WHEN latest.event_ts > hist.kafka_ts THEN 1 END)
            ELSE NULL
        END                                                              AS cookie_fraud_rate
    FROM (
        -- 1) latest session‐cookie per order
        SELECT
            data_weborder,
            event_ts,
            data_sessioncookie AS cookie_dim
        FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order
        WHERE
            event_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
            AND data_sessioncookie IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY data_weborder
            ORDER BY event_ts DESC NULLS LAST
        ) = 1
    ) latest
    LEFT JOIN (
        -- 2) latest historical record per order (to count all prior cookie events)
        SELECT
            k.data_weborder,
            CASE WHEN fo.etl_create_ts IS NOT NULL THEN fo.etl_create_ts ELSE k.event_ts END AS ts_for_join,
            k.event_ts                                                      AS kafka_ts,
            fo.etl_create_ts                                                AS fo_ts,
            k.data_sessioncookie                                            AS cookie_dim
        FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order k
        LEFT JOIN gbi_fraud_semantic_db.ai.fraud_order fo
          ON fo.web_order_id = k.data_weborder
        WHERE
            k.event_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
            AND k.data_sessioncookie IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY k.data_weborder
            ORDER BY k.event_ts DESC NULLS LAST
        ) = 1
    ) hist
      ON hist.cookie_dim    = latest.cookie_dim
     AND hist.ts_for_join   < latest.event_ts
    GROUP BY latest.data_weborder
)
SELECT * FROM cookie_fr2;


CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app.trevora_ip_fr2 AS
WITH ip_fr2 AS (
    SELECT
        latest.data_weborder,
        -- how many prior orders this IP has
        COUNT(CASE WHEN latest.event_ts > hist.kafka_ts THEN 1 END) AS ttl_prior_orders,
        -- how many of those were fraud
        COUNT(CASE WHEN latest.event_ts > hist.fo_ts    THEN 1 END) AS ttl_prior_fraud,
        -- fraud rate = prior frauds / prior orders, else -99
        CASE
            WHEN COUNT(CASE WHEN latest.event_ts > hist.kafka_ts THEN 1 END) > 0
            THEN COUNT(CASE WHEN latest.event_ts > hist.fo_ts THEN 1 END) * 100.0
                 / COUNT(CASE WHEN latest.event_ts > hist.kafka_ts THEN 1 END)
            ELSE NULL
        END                                                                       AS ip_fraud_rate
    FROM (
        -- 1) latest IP per order
        SELECT
            data_weborder,
            event_ts,
            data_ipaddress    AS ip_dim
        FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order
        WHERE
            event_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
            AND data_ipaddress IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY data_weborder
            ORDER BY event_ts DESC NULLS LAST
        ) = 1
    ) latest
    LEFT JOIN (
        -- 2) latest historical record per order (to count all prior IP events)
        SELECT
            k.data_weborder,
            CASE WHEN fo.etl_create_ts IS NOT NULL THEN fo.etl_create_ts ELSE k.event_ts END AS ts_for_join,
            k.event_ts                                                     AS kafka_ts,
            fo.etl_create_ts                                               AS fo_ts,
            k.data_ipaddress                                               AS ip_dim
        FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order k
        LEFT JOIN gbi_fraud_semantic_db.ai.fraud_order fo
          ON fo.web_order_id = k.data_weborder
        WHERE
            k.event_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
            AND k.data_ipaddress IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY k.data_weborder
            ORDER BY k.event_ts DESC NULLS LAST
        ) = 1
    ) hist
      ON hist.ip_dim       = latest.ip_dim
     AND hist.ts_for_join  < latest.event_ts
    GROUP BY latest.data_weborder
)
SELECT * FROM ip_fr2;


CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app.trevora_a1add_fr2 AS
WITH
  -- 1) Pull and normalize ship-to address components
  base_addr AS (
    SELECT
      k.data_weborder,
      k.event_ts,
      k.data_shiptocity   AS a1city,
      k.data_shiptostate  AS a1state,
      k.data_salesorg     AS sales_org,
      k.output_order_type AS order_type,
      TRIM(
        CONCAT_WS(' ',
          COALESCE(k.data_shiptostreetprefix, ''),
          COALESCE(k.data_shiptostreet,       ''),
          COALESCE(k.data_shiptocity,         '')
        )
      ) AS ship_to_address
    FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order k
    WHERE
      k.event_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
      AND k.output_digital_egc_model_eligible = 'false'
  ),

  annotated_addr AS (
    SELECT
      ba.data_weborder,
      ba.event_ts,
      ba.a1city,
      ba.a1state,
      ba.sales_org,
      ba.order_type,
      ba.ship_to_address,
      COUNT(*) OVER (
        PARTITION BY ba.a1city, ba.a1state, ba.sales_org, ba.order_type, ba.ship_to_address
        ORDER BY ba.event_ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ) AS ttl_prior_orders,
      COUNT(fo.web_order_id) OVER (
        PARTITION BY ba.a1city, ba.a1state, ba.sales_org, ba.order_type, ba.ship_to_address
        ORDER BY ba.event_ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ) AS ttl_prior_fraud
    FROM base_addr ba
    LEFT JOIN gbi_fraud_semantic_db.ai.fraud_order fo
      ON fo.web_order_id = ba.data_weborder
  ),

  -- 3) Pick the “latest” record for each order and compute the rate
  latest_per_order AS (
    SELECT
      data_weborder,
      ttl_prior_orders,
      ttl_prior_fraud,
      CASE
        WHEN ttl_prior_orders > 0
        THEN ttl_prior_fraud * 100.0 / ttl_prior_orders
        ELSE NULL
      END AS a1add_fraud_rate,
      ROW_NUMBER() OVER (
        PARTITION BY data_weborder
        ORDER BY event_ts DESC
      ) AS rn
    FROM annotated_addr
  )

SELECT
  data_weborder,
  ttl_prior_orders,
  ttl_prior_fraud,
  a1add_fraud_rate
FROM latest_per_order
WHERE rn = 1;



CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app.trevora_a1zip_amr_fr2 AS
WITH hist_zip AS (
  SELECT
    k.data_salesorg     AS sales_org,
    k.output_order_type AS order_type,
    CASE
      WHEN k.data_salesorg = '5600' THEN LEFT(k.data_shiptozip, 5)
      ELSE k.data_shiptozip
    END                                        AS a1zip,
    CASE
      WHEN fo.etl_create_ts IS NOT NULL THEN fo.etl_create_ts
      ELSE k.event_ts
    END                                        AS ts_for_join,
    k.event_ts                                 AS kafka_ts,
    fo.etl_create_ts                           AS fo_ts
  FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order k
  LEFT JOIN gbi_fraud_semantic_db.ai.fraud_order fo
    ON fo.web_order_id = k.data_weborder
  WHERE
    k.event_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
    AND k.output_top_level_model_type <> 'digital_egc'
    AND k.output_order_type <> 'adp'
    AND k.data_salesorg IS NOT NULL
    AND k.output_order_type IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      k.data_salesorg,
      k.output_order_type,
      CASE WHEN k.data_salesorg = '5600' THEN LEFT(k.data_shiptozip, 5) ELSE k.data_shiptozip END
    ORDER BY ts_for_join DESC
  ) = 1
),
latest_zip AS (
  SELECT
    data_weborder,
    event_ts,
    data_salesorg     AS sales_org,
    output_order_type AS order_type,
    CASE
      WHEN data_salesorg = '5600' THEN LEFT(data_shiptozip, 5)
      ELSE data_shiptozip
    END                AS a1zip
  FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order
  WHERE
    event_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
    AND output_top_level_model_type <> 'digital_egc'
    AND output_order_type <> 'adp'
    AND data_salesorg IS NOT NULL
    AND output_order_type IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY data_weborder
    ORDER BY event_ts DESC NULLS LAST
  ) = 1
),
a1zip_amr_fr2 AS (
  SELECT
    l.data_weborder,
    COUNT_IF(l.event_ts > h.kafka_ts) AS ttl_prior_orders,
    COUNT_IF(l.event_ts > h.fo_ts)    AS ttl_prior_fraud,
    CASE
      WHEN COUNT_IF(l.event_ts > h.kafka_ts) > 0
      THEN COUNT_IF(l.event_ts > h.fo_ts) * 100.0
           / COUNT_IF(l.event_ts > h.kafka_ts)
      ELSE NULL
    END                                   AS a1zip_fraud_rate
  FROM latest_zip l
  LEFT JOIN hist_zip h
    ON h.sales_org  = l.sales_org
   AND h.order_type = l.order_type
   AND h.a1zip      = l.a1zip
  GROUP BY l.data_weborder
)
SELECT * FROM a1zip_amr_fr2;



CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app.trevora_b2ed_fr2 AS
WITH domain_events AS (
  SELECT
    o.web_order_id                              AS data_weborder,
    o.event_ts,
    o.email_domain_name                         AS email_dim,
    -- how many prior orders for this domain
    SUM(1) OVER (
      PARTITION BY o.email_domain_name
      ORDER BY o.event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )                                           AS ttl_prior_orders,
    -- how many prior frauds for this domain
    SUM(CASE WHEN fo.web_order_id IS NOT NULL THEN 1 ELSE 0 END)
      OVER (
        PARTITION BY o.email_domain_name
        ORDER BY o.event_ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      )                                         AS ttl_prior_fraud
  FROM gbi_fraud_semantic_db.ai.order_summary o
  LEFT JOIN gbi_fraud_semantic_db.ai.fraud_order fo
    ON fo.order_id = o.order_id
  WHERE
    o.order_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
    AND o.email_domain_name IS NOT NULL
    -- only exclude the three problematic domains:
    AND o.email_domain_name NOT IN (
      'rock.apple.com',
      'globalrewardsolutions.com',
      'fullmasai.jp'
    )
)
SELECT
  data_weborder,
  ttl_prior_orders,
  ttl_prior_fraud,
  CASE
    WHEN ttl_prior_orders > 0
      THEN ttl_prior_fraud * 100.0 / ttl_prior_orders
    ELSE NULL
  END                                         AS b2ed_fraud_rate
FROM domain_events
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY data_weborder
    ORDER BY event_ts DESC
  ) = 1;



CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app.trevora_s2ed_fr2 AS
WITH domain_events AS (
  SELECT
    o.web_order_id                                        AS data_weborder,
    o.event_ts,
    SPLIT_PART(o.ship_to_cust_email_txt, '@', 2)          AS email_domain,
    SUM(1) OVER (
      PARTITION BY SPLIT_PART(o.ship_to_cust_email_txt, '@', 2)
      ORDER BY o.event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )                                                     AS ttl_prior_orders,
    SUM(
      CASE WHEN fo.web_order_id IS NOT NULL THEN 1 ELSE 0 END
    ) OVER (
      PARTITION BY SPLIT_PART(o.ship_to_cust_email_txt, '@', 2)
      ORDER BY o.event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )                                                     AS ttl_prior_fraud
  FROM gbi_fraud_semantic_db.ai.order_summary o
  LEFT JOIN gbi_fraud_semantic_db.ai.fraud_order fo
    ON fo.order_id = o.order_id
  WHERE
    o.order_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
    AND o.ship_to_cust_email_txt IS NOT NULL
    AND SPLIT_PART(o.ship_to_cust_email_txt, '@', 2)
        NOT IN ('rock.apple.com', 'globalrewardsolutions.com', 'fullmasai.jp')
)
SELECT
  data_weborder,
  ttl_prior_orders,
  ttl_prior_fraud,
  CASE
    WHEN ttl_prior_orders > 0
      THEN ttl_prior_fraud * 100.0 / ttl_prior_orders
    ELSE NULL
  END                                                   AS s2ed_fraud_rate
FROM domain_events
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY data_weborder
    ORDER BY event_ts DESC
  ) = 1;



CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app.trevora_a1nm_fr2 AS
WITH name_events AS (
  SELECT
    o.web_order_id                         AS data_weborder,
    o.event_ts,
    o.ship_to_cust_1_name                  AS a1nm_dim,

    -- count all prior events for this same ship‐to name
    SUM(1) OVER (
      PARTITION BY o.ship_to_cust_1_name
      ORDER BY o.event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )                                      AS ttl_prior_orders,

    -- count all prior fraud marks for this name
    SUM(
      CASE WHEN fo.web_order_id IS NOT NULL THEN 1 ELSE 0 END
    ) OVER (
      PARTITION BY o.ship_to_cust_1_name
      ORDER BY o.event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )                                      AS ttl_prior_fraud

  FROM gbi_fraud_semantic_db.ai.order_summary o
  LEFT JOIN gbi_fraud_semantic_db.ai.fraud_order fo
    ON fo.order_id = o.order_id
  WHERE
    o.order_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
    AND o.ship_to_cust_1_name IS NOT NULL
)

SELECT
  data_weborder,
  ttl_prior_orders,
  ttl_prior_fraud,
  CASE
    WHEN ttl_prior_orders > 0
      THEN ttl_prior_fraud * 100.0 / ttl_prior_orders
    ELSE NULL
  END                                    AS a1nm_fraud_rate
FROM name_events
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY data_weborder
    ORDER BY event_ts DESC
  ) = 1;


CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app.trevora_iap_fr2 AS
WITH events AS (
  SELECT
    k.data_weborder,
    k.event_ts,

    -- derive the IP+PO dimension once
    UPPER(
      CASE WHEN k.data_ipaddress LIKE '17.%' THEN 'apple_ip'
           ELSE REGEXP_SUBSTR(k.data_ipaddress, '[0-9]+\.[0-9]+\.[0-9]+')
      END
    ) || '_' || k.data_potype AS ipc_dim,

    -- count all prior orders for this same IP+PO combo
    COUNT(*) OVER (
      PARTITION BY
        UPPER(
          CASE WHEN k.data_ipaddress LIKE '17.%' THEN 'apple_ip'
               ELSE REGEXP_SUBSTR(k.data_ipaddress, '[0-9]+\.[0-9]+\.[0-9]+')
          END
        ) || '_' || k.data_potype
      ORDER BY k.event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS ttl_prior_orders,

    -- count all prior frauds for this same combo
    COUNT(fo.web_order_id) OVER (
      PARTITION BY
        UPPER(
          CASE WHEN k.data_ipaddress LIKE '17.%' THEN 'apple_ip'
               ELSE REGEXP_SUBSTR(k.data_ipaddress, '[0-9]+\.[0-9]+\.[0-9]+')
          END
        ) || '_' || k.data_potype
      ORDER BY k.event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS ttl_prior_fraud

  FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order k
  LEFT JOIN gbi_fraud_semantic_db.ai.fraud_order fo
    ON fo.web_order_id = k.data_weborder
  WHERE
    k.event_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
    AND k.data_ipaddress IS NOT NULL
    AND k.data_potype    IS NOT NULL
)

SELECT
  data_weborder,
  ttl_prior_orders,
  ttl_prior_fraud,
  CASE
    WHEN ttl_prior_orders > 0
    THEN ttl_prior_fraud * 100.0 / ttl_prior_orders
    ELSE NULL
  END AS iap_fraud_rate

FROM events
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY data_weborder
  ORDER BY event_ts DESC
) = 1;



CREATE OR REPLACE TABLE gbi_fraud_bap_db.ai_live_biz_app.trevor_allen_ek5 AS

WITH
  ekata_match AS (
    SELECT DISTINCT
      TRIM(request_weborder) AS ek_weborder
    FROM gbi_fraud_semantic_db.ai.acmng_ekata_arcadianr
    WHERE
      request_address_type      = 'billing_shipping'
      AND primary_address_to_name = 'match'
  ),

  taws AS (
    SELECT
      kw.data_weborder,
      kw.event_ts,
      kw.decisions_fraudstatus,
      kw.EVENT_DT,
      kw.output_amt / 100.00                                                   AS output_amt,
      (kw.output_dollar_scaled_fraud_score_adj::FLOAT
       - kw.output_dollar_scaled_threshold::FLOAT)                             AS distance_from_threshold,
      kw.output_order_type,
      kw.data_payment_avsdisp,
      kw.data_billtostreetprefix, kw.data_billtostreet,
      kw.data_billtocity,        kw.data_billtostate,
      kw.data_shiptostreetprefix, kw.data_shiptostreet,
      kw.data_shiptocity,         kw.data_shiptostate,
      em.ek_weborder                                                           AS ek_weborder
    FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order AS kw
    LEFT JOIN ekata_match AS em
      ON TRIM(kw.data_weborder) = em.ek_weborder
    WHERE
      kw.decisions_fraudstatus = 'REVIEW'
      AND kw.event_dt BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 90
      AND kw.output_order_type IN ('idl', 'sds', 'other')
      AND kw.output_digital_egc_model_eligible = 'false'
      AND kw.data_weborder IS NOT NULL
      AND kw.data_weborder NOT LIKE 'J%'
      AND TRIM(LOWER(CONCAT_WS(' ',
        kw.data_billtostreetprefix, kw.data_billtostreet,
        kw.data_billtocity,        kw.data_billtostate
      ))) = TRIM(LOWER(CONCAT_WS(' ',
        kw.data_shiptostreetprefix, kw.data_shiptostreet,
        kw.data_shiptocity,         kw.data_shiptostate
      )))
      AND (
        ARRAY_CONTAINS('YY'::VARIANT, kw.data_payment_avsdisp)
        OR em.ek_weborder IS NOT NULL
      )
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY kw.data_weborder
        ORDER BY kw.event_ts DESC
      ) = 1
  ),

  ekata AS (
    SELECT
      TRIM(request_weborder)     AS ek_weborder,
      primary_address_to_name    AS ek_b2add_b2nm,
      secondary_email_valid      AS ek_a1em_valid,
      event_date                 AS ek_event_date
    FROM gbi_fraud_semantic_db.ai.acmng_ekata_arcadianr
    WHERE
      request_address_type       = 'billing_shipping'
      AND primary_address_to_name = 'match'
  )

SELECT
  t.data_weborder                   AS won,
  t.data_payment_avsdisp,
  t.EVENT_DT,
  t.distance_from_threshold,
  t.output_amt,

  cc.cc_fraud_rate,
  b2em.b2em_fraud_rate,
  a1em.a1em_fraud_rate,
  cookie.cookie_fraud_rate,
  ip.ip_fraud_rate,
  a1add.a1add_fraud_rate,
  a1zip.a1zip_fraud_rate,
  bin.bin_fraud_rate,
  b2ed.b2ed_fraud_rate,
  s2ed.s2ed_fraud_rate,
  a1nm.a1nm_fraud_rate,
  iap.iap_fraud_rate,

  ek.ek_b2add_b2nm      AS ekata_address_match,
  ek.ek_a1em_valid      AS ekata_email_valid,
  ek.ek_event_date      AS ekata_event_date

FROM taws t
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trevora_cc_fr2        cc   ON cc.data_weborder = t.data_weborder
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trevora_b2em_fr2      b2em ON b2em.data_weborder = t.data_weborder
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trevora_a1em_fr2      a1em ON a1em.data_weborder = t.data_weborder
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trevora_cookie_fr2    cookie ON cookie.data_weborder = t.data_weborder
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trevora_ip_fr2        ip   ON ip.data_weborder = t.data_weborder
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trevora_a1add_fr2     a1add ON a1add.data_weborder = t.data_weborder
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trevora_a1zip_amr_fr2 a1zip ON a1zip.data_weborder = t.data_weborder
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trevora_bin_fr2       bin  ON bin.data_weborder = t.data_weborder
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trevora_b2ed_fr2      b2ed ON b2ed.data_weborder = t.data_weborder
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trevora_s2ed_fr2      s2ed ON s2ed.data_weborder = t.data_weborder
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trevora_a1nm_fr2      a1nm ON a1nm.data_weborder = t.data_weborder
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trevora_iap_fr2       iap  ON iap.data_weborder = t.data_weborder
LEFT JOIN ekata ek
  ON TRIM(t.data_weborder) = ek.ek_weborder;