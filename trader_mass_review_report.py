from ai.snowflake.compat.connection import sf
import argparse



def category_count_pool():
    q = """
    CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_category_count_pool_temp AS
 (
  WITH valid_item_pool AS (
       SELECT *
         FROM ( SELECT web_order_id
                     , order_id
                     , create_ts
                     , order_item_nr
                     , category
                     , prod_id
                     , campaign_id
                     , order_qty
                     , item_desc
                     , category_type
                  FROM gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_category_central_pool_vw
                 WHERE 1 = 1
                   AND TRIM(rejection_cd) = '' -- to exclude cancelled items
                    -- to group the same item from same order and filter to grab the latest line
                QUALIFY ROW_NUMBER() OVER (PARTITION BY web_order_id, order_item_nr ORDER BY create_ts DESC NULLS LAST) = 1 )
        WHERE order_id LIKE 'A%' -- to exclude return orders
          AND category IS NOT NULL  -- no need to count the category IS NULL
      ),

  total_counting_pool AS (
      SELECT pool.*
           , kafka.data_salesorg                                     AS sales_org
           , kafka.output_sales_district                             AS sales_district
           , kafka.data_billtophonenumber                            AS b2_ph
           , kafka.data_billtoemail                                  AS b2_em
           , kafka.data_shiptoemail                                  AS a1_em
           , kafka.data_personid                                     AS dsid
           , kafka.data_pcprint                                      AS guid
           , kafka.data_sessioncookie                                AS session_cookie
           , kafka.output_payment_id                                 AS payment_id
           , TRIM(kafka.data_payment_cardnumberhash[0], '"')         AS card_hash
           , kafka.data_ipaddress                                    AS ip_add
           , aps.sessionidentifier_milvetid                          AS student_id
        FROM valid_item_pool pool
   LEFT JOIN ( SELECT data_salesorg
                    , output_sales_district
                    , data_weborder
                    , data_billtophonenumber
                    , data_billtoemail
                    , data_shiptoemail
                    , data_personid
                    , data_pcprint
                    , data_sessioncookie
                    , output_payment_id
                    , data_payment_cardnumberhash
                    , data_ipaddress
                    , output_checkoutsessionid
                 FROM gbi_fraud_semantic_db.ai.kafka_athena_nvp_aos_web_order
                WHERE 1 =1
                  AND event_dt >= DATEADD(YEAR, -1,DATE_TRUNC('YEAR', current_date))
                  AND data_weborder LIKE 'W%'
               QUALIFY ROW_NUMBER () OVER (PARTITION BY data_weborder ORDER BY event_ts DESC NULLS LAST) = 1
             ) kafka
            ON pool.web_order_id = kafka.data_weborder
   LEFT JOIN ( SELECT event_ts, event_dt, checkoutsessionid, sessionidentifier_milvetid
                 FROM GBI_FRAUD_SEMANTIC_DB.AI.kafka_athena_nvp_aos_place_order_short_retn
                WHERE 1=1
                  AND event_ts>=DATE_TRUNC('YEAR', current_date)
               QUALIFY ROW_NUMBER () OVER (PARTITION BY checkoutsessionid ORDER BY event_ts DESC NULLS LAST) = 1
              ) aps
            ON aps.checkoutsessionid = kafka.output_checkoutsessionid
   )

--- add the strategy filter
SELECT tcp.*
  FROM total_counting_pool  tcp
LEFT JOIN ( SELECT DISTINCT prod_id
                         , sales_org_cd
                         , trader_strategy_cd
              FROM gbi_fraud_bap_db.ai_live_biz_app.trader_product_strategy_published_source
             WHERE sales_district_cd NOT IN ('IN17','IN18','IN19','IN25','IN26','IN27','RW17','RW18','RW19','RW25','RW26','RW27')
            ) tpst
       ON tcp.prod_id = tpst.prod_id AND  tcp.sales_org = tpst.sales_org_cd
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_edu_strategy_published_source tesp
        ON tcp.category_type =2 AND tcp.category = tesp.edu_cate
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_promo_strategy_published_source tpsp
       ON tcp.campaign_id = tpsp.campaign_id
    WHERE 
    tpst.trader_strategy_cd <> 'RELEASE'
       OR tesp.trader_strategy_cd <> 'RELEASE'
       OR tpsp.trader_strategy_cd <> 'RELEASE'
    ) ;
    """.strip()
    print(q)
    sf.run_query(q)


def create_max_columns(categories: set,datapoint_list,exclude_datapoint_list):

    # Create category total max columns and current orders item count
    max_columns = []
    for category in sorted(categories):
        max_lines = []
        for datapoint in datapoint_list:
            if datapoint not in exclude_datapoint_list:
                max_line = f"""COALESCE(CAST(TRIM(SPLIT_PART(GET_PATH({datapoint}.item_count, '{category}'), ',', 1)) AS INTEGER),0)"""
                max_lines.append(max_line)
            else:
                pass
        max_column = f", GREATEST({', '.join(max_lines)}) AS {category}_total_max"
        max_columns.append(max_column)

    max_columns_sql = "\n".join(max_columns)

    return max_columns_sql


def create_exceeded_ind(datapoint_list,exclude_datapoint_list):

    conditions = [
        f"""
        TRIM(
          SPLIT(
            GET_PATH({datapoint}.item_count, '["' || REPLACE(category.key, '"', '\\\\\"') || '"]')::STRING, 
           ','
           )[1]
          ) = 'Y'
        """
        for datapoint in datapoint_list
        if datapoint not in exclude_datapoint_list
    ]
    or_clause = " OR\n        ".join(conditions)

    exceeded_ind_sql = f"""
                                CASE 
                                  WHEN category.KEY != '_dummy' AND (       
                             {or_clause}    )
                                  THEN 'Y' ELSE 'N'
                                END AS exceeded_ind
                        """
    return exceeded_ind_sql


def join_all(categories,datapoint_list,exclude_datapoint_list):
    max_columns = create_max_columns(categories,datapoint_list,exclude_datapoint_list)
    exceeded_ind_sql = create_exceeded_ind(datapoint_list,exclude_datapoint_list)

    # Prepare velocity columns
    velocity_tables = ["b2_em_dom","a1_em_dom","card_bin","card_issuer","ip_company","b2_zip","a1_zip"]

    velocity_columns = [f", COALESCE({field_nm}.total_orders,0) AS {field_nm}_velocity"
                        for field_nm in velocity_tables
                        ]
    velocity_columns_sql = "\n".join(velocity_columns)

    # Prepare category columns
    category_columns = [f", {datapoint}.item_count as {datapoint}_item_count"
                        for datapoint in datapoint_list
                        ]
    category_columns_sql = "\n".join(category_columns)

    # SQL query for final join and create table for final results
    query1 = """
             CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_ww_mr_base_vw_temp AS (
                SELECT *, 
                     CASE
                       WHEN current_category_count IS NULL OR ARRAY_SIZE(OBJECT_KEYS(current_category_count)) = 0 THEN PARSE_JSON('{"_dummy": 0}') 
                       ELSE current_category_count
                     END AS category_filled
                 FROM gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_ww_mr_base_vw
                );
            """

    query2 = f"""
            CREATE OR REPLACE TABLE gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_ww_mass_review AS (
                SELECT b.* EXCLUDE (category_filled) 
                     , {exceeded_ind_sql} 
                     --max total columns
                       {max_columns}
                     -- agg category count columns
                       {category_columns_sql}
                     --velocity columns
                       {velocity_columns_sql}
                     , c5.ct_rate
                     , svw.HAS_PROD_CANCEL
                     , svw.HAS_PROD_REQUOTE 
                     , svw.HAS_PROMO_CANCEL 
                     , svw.HAS_PROMO_REQUOTE
                     , svw.CANCEL_PROD_LIST 
                     , svw.REQUOTE_PROD_LIST 
                     , svw.CANCEL_PROMO_LIST 
                     , svw.REQUOTE_PROMO_LIST
                  FROM gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_ww_mr_base_vw_temp  b
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_5core_ct_rate_vw c5
                    ON b.sales_district = c5.sales_district AND b.card_bin = c5.card_bin AND b.po_type = c5.po_type AND b.ip_company = c5.ip_company AND b.b2_em_dom = c5.b2_em_dom
                --category count tables join 
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_session_cookie_category_counts session_cookie 
                    ON b.sales_org = session_cookie.sales_org AND b.session_cookie = session_cookie.session_cookie
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_b2_em_category_counts b2_em 
                    ON b.sales_org = b2_em.sales_org AND b.b2_em = b2_em.b2_em
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_a1_em_category_counts a1_em 
                    ON b.sales_org = a1_em.sales_org AND b.a1_em = a1_em.a1_em
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_guid_category_counts guid 
                    ON b.sales_org = guid.sales_org AND b.guid = guid.guid
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_dsid_category_counts dsid 
                    ON b.sales_org = dsid.sales_org AND b.dsid = dsid.dsid
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_card_hash_category_counts card_hash 
                    ON b.sales_org = card_hash.sales_org AND b.card_hash = card_hash.card_hash
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_payment_id_category_counts payment_id 
                    ON b.sales_org = payment_id.sales_org AND b.payment_id = payment_id.payment_id
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_student_id_category_counts student_id 
                    ON b.sales_org = student_id.sales_org AND b.student_id = student_id.student_id
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_b2_ph_category_counts b2_ph 
                    ON b.sales_org = b2_ph.sales_org AND b.b2_ph = b2_ph.b2_ph
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_ip_add_category_counts ip_add 
                    ON b.sales_org = ip_add.sales_org AND b.ip_add = ip_add.ip_add
                --velocity tables join
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_b2_em_dom_velocity_vw b2_em_dom 
                    ON b.sales_org = b2_em_dom.sales_org AND b.b2_em_dom = b2_em_dom.b2_em_dom
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_a1_em_dom_velocity_vw a1_em_dom 
                    ON b.sales_org = a1_em_dom.sales_org AND b.a1_em_dom = a1_em_dom.a1_em_dom
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_card_bin_velocity_vw card_bin 
                    ON b.sales_org = card_bin.sales_org AND b.card_bin[0] = card_bin.card_bin
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_card_issuer_velocity_vw card_issuer 
                    ON b.sales_org = card_issuer.sales_org AND b.card_issuer[0] = card_issuer.card_issuer
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_ip_company_velocity_vw ip_company 
                    ON b.sales_org = ip_company.sales_org AND b.ip_company = ip_company.ip_company
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_b2_zip_velocity_vw b2_zip 
                    ON b.sales_org = b2_zip.sales_org AND b.b2_zip = b2_zip.b2_zip
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_a1_zip_velocity_vw a1_zip 
                    ON b.sales_org = a1_zip.sales_org AND b.a1_zip = a1_zip.a1_zip
                  LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_order_id_with_strategy_vw svw 
                    ON b.order_id = svw.order_id
                --strategy view join
                  LEFT JOIN LATERAL FLATTEN(input =>b.category_filled) AS category 
               QUALIFY ROW_NUMBER() OVER (PARTITION BY b.order_id ORDER BY exceeded_ind DESC NULLS LAST) = 1 
                )
               """
    print(query1)
    sf.run_query(query1)  # for testing purposes, please delete later

    print(query2)
    sf.run_query(query2)


def make_view_to_temp(view_name):
    sf.run_query("USE ROLE GBI_FRAUD_BAP_DB_AI_LIVE_BIZ_APP_MAIN_ROLE")
    query = f"""
    CREATE OR REPLACE TEMPORARY TABLE gbi_fraud_bap_db.ai_live_biz_app.{view_name}_temp AS (
       SELECT * FROM gbi_fraud_bap_db.ai_live_biz_app.{view_name}
    )
    """
    print(query)
    sf.run_query(query)


def create_categories_count_table(datapoint_list):
    for datapoint in datapoint_list:
        query = f"""
          CREATE OR REPLACE TABLE gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_{datapoint}_category_counts AS (
                WITH {datapoint}_count   AS ( SELECT sales_org
                                       , {datapoint}
                                       , category
                                       , SUM(order_qty) AS category_count
                                    FROM gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_category_count_pool_temp
                                   WHERE 1 = 1
                                     AND {datapoint} IS NOT NULL
            """
        if datapoint == 'guid':
            query += """ AND guid <> 'U' 
                         """
        if datapoint == 'session_cookie' or datapoint == 'ip_add':
            query += """ AND NOT ip_add LIKE ANY ('17.%%','10.82.%%', '144.178%%')      
                         AND sales_district NOT LIKE 'RK%'                             
                         """
        if datapoint == 'card_hash':
            query += """ AND card_hash NOT LIKE '11111111%%'
                         """
        query += f"""
                                   GROUP BY 1, 2, 3 )
              , {datapoint}_exceeds AS ( SELECT c.*
                                       , CASE WHEN c.category_count > pl.lifetime_limit THEN 'Y'
                                              WHEN c.category_count > cl.lifetime_limit THEN 'Y'
                                              WHEN c.category_count > el.epp_limit THEN 'Y'
                                         ELSE 'N'
                                          END AS exceeds
                                    FROM {datapoint}_count c
                                    LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_lifetime_limit_promo_vw_temp pl
                                        ON c.category = pl.category
                                    LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.read_trader_lifetime_limit_vw_temp cl
                                        ON c.category = cl.category AND c.sales_org = cl.sales_org
                                    LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_edu_limit_vw_temp el
                                        ON c.category = el.edu_class
                                   WHERE category_count > 0 )

            SELECT sales_org
                 , {datapoint}
                 , OBJECT_AGG(category, TO_VARIANT(ROUND (category_count,0) || ', ' || exceeds)) AS item_count
            FROM {datapoint}_exceeds
            GROUP BY 1, 2 );

            """
        print(query)
        sf.run_query(query)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--category_list", nargs="+", default=['bts_promo_ipad_giftcard_202406'])
    args = parser.parse_args()
    categories = set(args.category_list)

    category_count_pool()

    # load limit in temp table
    make_view_to_temp("trader_lifetime_limit_promo_vw")
    make_view_to_temp("read_trader_lifetime_limit_vw")
    make_view_to_temp("trader_tfa_edu_limit_vw")

    # List of datapoints to create category count tables, exceed logic and max total columns
    datapoint_list = ["session_cookie", "b2_em", "a1_em", "guid", "dsid", "card_hash", "payment_id", "student_id",
                      "b2_ph", "ip_add"]
    # List of datapoint to ignore for exceed logic and max total columns
    exclude_datapoint_list = ["b2_ph", "ip_add"]

    create_categories_count_table(datapoint_list)

    join_all(categories,datapoint_list,exclude_datapoint_list)

if __name__ == "__main__":
    main()
