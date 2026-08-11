# trader_alerts_adhoc_task.py - TJH pipeline using review frame for ad hoc orders
from ai.snowflake.compat.connection import sf
import pandas as pd
import re
import time
from snowflake.connector.pandas_tools import write_pandas


# ────────────────────────────────────────────────────────────────
# AD HOC ORDER SET — change these filters as needed
# ────────────────────────────────────────────────────────────────
ADHOC_ORDERS_SQL = """
    SELECT DISTINCT order_id
    FROM GBI_FRAUD_BAP_DB.AI_LIVE_BIZ_APP.TRADER_THRESHOLDS_DTL
    WHERE STOREFRONTDATA_COUNTRYCODE = 'US'
      AND EVENT_DT BETWEEN '2026-02-14' AND '2026-02-15'
      AND BOPIS_ITEM_CATEG_CD = 'SFS'
"""


def get_active_patterns():
    """Get active patterns with query types"""
    query = """
        SELECT table_name, query_type
        FROM gbi_fraud_bap_db.ai_live_biz_app.trader_hunting_patterns
        WHERE activity = 'Active'
        ORDER BY query_type, table_name
    """
    df = sf.run_query(query)
    return df.to_dict('records')


def get_trader_hunting_query(hunting_pattern):
    """Get DDL for trader hunting view"""
    original_query = f"""
    SELECT get_ddl('view', 'gbi_fraud_bap_db.ai_live_biz_app.{hunting_pattern}') AS query
    """
    df = sf.run_query(original_query)
    return df['query'][0]


def check_column_exists(query_text, column_name):
    """Check if a column name appears in the pattern query text"""
    return bool(re.search(rf'\b{column_name}\b', query_text, re.IGNORECASE))


def transform_query(query):
    """Universal query transformer — extracts SELECT from view DDL"""
    try:
        if not query:
            return None

        as_match = re.search(r'\bAS\s+(.*)', query, re.IGNORECASE | re.DOTALL)
        if not as_match:
            print("❌ Could not find AS clause in query")
            return None

        extracted = as_match.group(1).strip()

        if extracted.startswith('(') and extracted.endswith(')'):
            extracted = extracted[1:-1].strip()

        extracted = re.sub(r';+\s*$', '', extracted)

        transformed = re.sub(
            r"gbi_trader_semantic_db\.ai\.trader_orders",
            r"gbi_fraud_bap_db.ai_live_biz_app.trader_orders",
            extracted,
            flags=re.IGNORECASE
        )

        return transformed

    except Exception as e:
        print(f"❌ Transform error: {e}")
        return None


def create_adhoc_orders_table():
    """Create temp table of ad hoc order_ids"""
    print("Creating ad hoc orders table...")
    sf.run_query("DROP TABLE IF EXISTS adhoc_task_orders")

    sql = f"""
    CREATE TEMP TABLE adhoc_task_orders AS (
        {ADHOC_ORDERS_SQL}
    )
    """
    start_time = time.time()
    sf.run_query(sql)
    duration = time.time() - start_time

    count_df = sf.run_query("SELECT COUNT(*) AS cnt FROM adhoc_task_orders")
    order_count = count_df['cnt'][0]
    print(f"Ad hoc orders table created: {order_count} orders in {duration:.2f}s")
    return duration, order_count


def create_temp_table():
    """Create temp table from REVIEW FRAME filtered to ad hoc orders"""
    print("Creating temporary frame table from REVIEW FRAME (filtered to ad hoc orders)...")

    sf.run_query("DROP TABLE IF EXISTS trader_temp_frame")

    # Source from review frame, join on ORDER_ID
    sql = """
    CREATE TEMP TABLE trader_temp_frame AS
    SELECT DISTINCT trf.*
    FROM gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_review_frame_vw trf
    INNER JOIN adhoc_task_orders ao
        ON trf.ORDER_ID = ao.order_id
    """

    start_time = time.time()
    sf.run_query(sql)
    duration = time.time() - start_time

    count_df = sf.run_query("SELECT COUNT(*) AS cnt FROM trader_temp_frame")
    frame_count = count_df['cnt'][0]
    print(f"Temp frame created: {frame_count} rows in {duration:.2f}s")

    if frame_count == 0:
        print("⚠️ 0 rows matched! Running diagnostics...")
        diag = sf.run_query("""
            SELECT ao.order_id::VARCHAR AS sample_id
            FROM adhoc_task_orders ao LIMIT 5
        """)
        print(f"  Sample ad hoc order_ids: {diag['sample_id'].tolist()}")

        diag2 = sf.run_query("""
            SELECT trf.ORDER_ID::VARCHAR AS trf_order_id
            FROM gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_review_frame_vw trf
            LIMIT 5
        """)
        print(f"  Sample review frame ORDER_IDs: {diag2['trf_order_id'].tolist()}")

    return duration


def create_results_table():
    """Create results table"""
    sql = """
    CREATE OR REPLACE TABLE gbi_fraud_bap_db.ai_live_biz_app.Trader_Automated_Queries_Results (
        ORDER_ID VARCHAR(16777216),
        WEB_ORDER_ID VARCHAR(16777216),
        hunting_pattern VARCHAR(255),
        query_type VARCHAR(10),
        ACTION_LEVEL VARCHAR(50),
        EVENT_TS DATE,
        COMMIT_CD VARCHAR(4),
        COMMIT_DT DATE,
        SALES_ORG VARCHAR(16777216),
        PO_TYPE VARCHAR(16777216),
        SALES_DISTRICT VARCHAR(16777216),
        DISCOUNT_STORE_TYPE VARCHAR(13),
        SLA VARCHAR(16777216),
        EXCEPTION_CD VARCHAR(16777216),
        B2_NM VARCHAR(16777216),
        B2_EM VARCHAR(16777216),
        B2_EM_DOM VARCHAR(16777216),
        ACCT_AGE VARCHAR(16777216),
        B2_ADD VARCHAR(16777216),
        B2_CITY VARCHAR(16777216),
        B2_DISTRICT VARCHAR(16777216),
        B2_STATE VARCHAR(16777216),
        B2_ZIP VARCHAR(16777216),
        B2_PH VARCHAR(16777216),
        A1_NM VARCHAR(16777216),
        A1_EM VARCHAR(16777216),
        A1_EM_DOM VARCHAR(16777216),
        A1_ADD VARCHAR(16777216),
        A1_CITY VARCHAR(16777216),
        A1_DISTRICT VARCHAR(16777216),
        A1_STATE VARCHAR(16777216),
        A1_ZIP VARCHAR(16777216),
        A1_PHONE VARCHAR(16777216),
        LATEST_A1_NM VARCHAR(16777216),
        LATEST_A1_EM VARCHAR(16777216),
        LASTEST_A1_EM_DOM VARCHAR(16777216),
        LATEST_A1_ADD VARCHAR(16777216),
        LATEST_A1_STATE VARCHAR(16777216),
        LATEST_A1_ZIP VARCHAR(16777216),
        LATEST_BOPIS3_NM VARIANT,
        LATEST_BOPIS3_EM VARIANT,
        PROD_DESC VARCHAR(16777216),
        TOTAL_VALUE NUMBER(38,2),
        IP_ADD VARCHAR(16777216),
        IP_CLASS VARCHAR(16777216),
        IP_COMPANY VARCHAR(16777216),
        IP_COUNTRY VARCHAR(16777216),
        IP_BIN_COUNTRY VARCHAR(16777216),
        PAYMENT_TYPES VARCHAR(16777216),
        CARD_BIN VARCHAR(16777216),
        BIN_COUNTRY VARCHAR(16777216),
        CARD_HASH VARCHAR(16777216),
        CARD_ISSUER VARCHAR(16777216),
        GUID VARCHAR(16777216),
        SESSION_COOKIE VARCHAR(16777216),
        order_identifier VARCHAR(16777216),
        CREATED_TS TIMESTAMP
    )
    """
    sf.run_query(sql)


def execute_pattern_with_enrichment(pattern_name, query_type):
    """Execute pattern, enrich from review frame columns"""
    try:
        print(f"  🚀 {pattern_name}: ", end="")

        query = get_trader_hunting_query(pattern_name)
        transformed_query = transform_query(query)

        if not transformed_query:
            print("Could not extract query")
            return pd.DataFrame(), 0

        # Replace traderhunt frame references with our temp table (sourced from review frame)
        transformed_query = re.sub(
            r'gbi_fraud_bap_db\.ai_live_biz_app\.Trader_TFA_traderhunt_frame',
            'trader_temp_frame',
            transformed_query,
            flags=re.IGNORECASE
        )

        # ACTION_LEVEL logic
        if query_type == 'GCT':
            has_action_level = check_column_exists(transformed_query, 'action_level')
            if has_action_level:
                action_level_sql = "COALESCE(t.action_level, 'REVIEW')"
                select_action_level = ", action_level"
            else:
                action_level_sql = "'REVIEW'"
                select_action_level = ""
        else:
            if 'AMR' in pattern_name.upper():
                action_level_sql = "'HIGH_RISK'"
            else:
                action_level_sql = "'REVIEW'"
            select_action_level = ""

        # ── ENRICHMENT SELECT ──
        # Maps review frame columns to results table columns
        # NULLs for columns that don't exist in review frame
        enriched_query = f"""
        SELECT 
            t.order_identifier,
            '{pattern_name}' AS hunting_pattern,
            '{query_type}' AS query_type,
            {action_level_sql} AS ACTION_LEVEL,
            tf.ORDER_ID, 
            tf.WEB_ORDER_ID,
            tf.EVENT_TS,
            NULL AS COMMIT_CD,
            NULL AS COMMIT_DT,
            tf.SALES_ORG,
            tf.PO_TYPE,
            tf.SALES_DISTRICT,
            tf.DISCOUNT_STORE_TYPE,
            tf.SLA,
            NULL AS EXCEPTION_CD,
            tf.B2_NM,
            tf.B2_EM,
            tf.B2_EM_DOM,
            tf.ACCT_AGE,
            tf.B2_ADD,
            tf.B2_CITY,
            tf.B2_DISTRICT,
            tf.B2_STATE,
            tf.B2_ZIP,
            tf.B2_PH,
            tf.A1_NM,
            tf.A1_EM,
            tf.A1_EM_DOM,
            tf.A1_ADD,
            tf.A1_CITY,
            tf.A1_DISTRICT,
            tf.A1_STATE,
            tf.A1_ZIP,
            tf.A1_PH AS A1_PHONE,
            tf.LATEST_A1_NM,
            tf.LATEST_A1_EM,
            NULL AS LASTEST_A1_EM_DOM,
            tf.LATEST_A1_ADD,
            tf.LATEST_A1_STATE,
            tf.LATEST_A1_ZIP,
            tf.LATEST_BOPIS3_NM,
            tf.LATEST_BOPIS3_EM,
            tf.PROD_DESC,
            tf.TOTAL_VALUE,
            tf.IP_ADD,
            tf.IP_CLASS,
            tf.IP_COMPANY,
            tf.IP_COUNTRY,
            NULL AS IP_BIN_COUNTRY,
            tf.PAYMENT_TYPE AS PAYMENT_TYPES,
            tf.CARD_BIN,
            tf.BIN_COUNTRY,
            tf.CARD_HASH,
            tf.CARD_ISSUER,
            tf.GUID,
            tf.SESSION_COOKIE,
            CURRENT_TIMESTAMP AS CREATED_TS
        FROM (
            SELECT 
                COALESCE(
                    TRY_CAST(web_order_id AS VARCHAR), 
                    TRY_CAST(order_id AS VARCHAR),
                    'UNKNOWN'
                ) AS order_identifier
                {select_action_level}
            FROM ({transformed_query})
            WHERE COALESCE(
                TRY_CAST(web_order_id AS VARCHAR), 
                TRY_CAST(order_id AS VARCHAR)
            ) IS NOT NULL
        ) t
        LEFT JOIN trader_temp_frame tf
            ON t.order_identifier = COALESCE(tf.WEB_ORDER_ID, tf.ORDER_ID::VARCHAR)
        INNER JOIN adhoc_task_orders ao
            ON t.order_identifier = ao.order_id::VARCHAR
        """

        start_time = time.time()
        result = sf.run_query(enriched_query)
        duration = time.time() - start_time

        row_count = len(result) if not result.empty else 0
        print(f"{row_count} orders in {duration:.3f}s")

        return result, row_count

    except Exception as e:
        error_msg = str(e)
        # Show concise error — highlight if it's a missing column issue
        if 'invalid identifier' in error_msg.lower():
            col_match = re.search(r"invalid identifier '(\w+)'", error_msg, re.IGNORECASE)
            col_name = col_match.group(1) if col_match else '?'
            print(f"SKIPPED — column '{col_name}' not in review frame")
        else:
            print(f"ERROR - {error_msg[:200]}")
        return pd.DataFrame(), 0


def write_to_sf_table(combined_results):
    """Write results to Snowflake with explicit commit"""
    if combined_results.empty:
        print("❌ No data to write - DataFrame is empty")
        return False

    try:
        print(f"🚀 Writing {len(combined_results)} rows to results table...")

        sf.run_query("TRUNCATE TABLE gbi_fraud_bap_db.ai_live_biz_app.Trader_Automated_Queries_Results")

        combined_results = combined_results.reset_index(drop=True)

        conn = sf._backend._engine.connect()

        result = write_pandas(
            conn=conn.connection,
            df=combined_results,
            database='gbi_fraud_bap_db',
            schema='ai_live_biz_app',
            table_name='Trader_Automated_Queries_Results',
            quote_identifiers=False,
            auto_create_table=False,
            use_logical_type=True
        )

        conn.connection.commit()
        conn.close()

        print(f"✅ Successfully wrote {len(combined_results)} rows")
        return True

    except Exception as e:
        print(f"❌ Write failed: {e}")
        return False


def cleanup_temp_tables():
    """Clean up all temporary tables"""
    sf.run_query("DROP TABLE IF EXISTS trader_temp_frame")
    sf.run_query("DROP TABLE IF EXISTS adhoc_task_orders")


def format_slack_message(pattern_metrics, skipped_patterns, total_count, gct_count, th_count, total_runtime, adhoc_order_count):
    """Format Slack message — ad hoc version"""

    message = "🚨 Trader Alerts Results (AD HOC TASK)\n"
    message += "━━━━━━━━━━━━━━━━━━━━━━\n"
    message += f"📋 Scoped to {adhoc_order_count} ad hoc orders (US / SFS / 2026-02-14–15)\n"
    message += f"📂 Source: trader_tfa_review_frame_vw\n\n"

    gct_patterns = {k: v for k, v in pattern_metrics.items() if
                    k.startswith('trader_genuine') or k.startswith('trader_good')}
    if gct_patterns:
        message += "✅ Genuine Customer Queries\n"
        for pattern, count in gct_patterns.items():
            if count > 0:
                message += f"  {pattern}: {count} orders\n"
        message += "\n"

    th_patterns = {k: v for k, v in pattern_metrics.items() if k not in gct_patterns}
    if th_patterns:
        message += "🔍 Trader Hunting Queries\n"
        for pattern, count in th_patterns.items():
            if count > 0:
                message += f"  {pattern}: {count} orders\n"
        message += "\n"

    if skipped_patterns:
        message += "⚠️ Skipped (column mismatch with review frame)\n"
        for pattern in skipped_patterns:
            message += f"  {pattern}\n"
        message += "\n"

    message += "━━━━━━━━━━━━━━━━━━━━━━\n"
    message += f"📊 Total: {total_count} orders\n"
    message += f"⏱️ Runtime: {total_runtime:.1f}s\n\n"
    message += f"✅ GCT: {gct_count} | 🔍 TH: {th_count}\n\n"
    message += "SELECT * FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_Automated_Queries_Results"

    return message


def main():
    print("🚀 TRADER HUNTING PIPELINE - AD HOC TASK (REVIEW FRAME)")
    print("=" * 55)

    total_start_time = time.time()

    try:
        # 1. Create ad hoc orders scope
        adhoc_time, adhoc_order_count = create_adhoc_orders_table()

        if adhoc_order_count == 0:
            print("⚠️ No ad hoc orders found — check your filters. Exiting.")
            return

        # 2. Get active patterns
        patterns = get_active_patterns()
        print(f"Found {len(patterns)} active patterns")

        # 3. Create results table
        create_results_table()

        # 4. Create temp frame from REVIEW FRAME (filtered to ad hoc orders)
        temp_creation_time = create_temp_table()

        # 5. Execute each pattern
        print("\nExecuting patterns against ad hoc orders:")
        all_results = []
        pattern_metrics = {}
        skipped_patterns = []
        total_pattern_time = 0

        for pattern in patterns:
            pattern_name = pattern['table_name']
            query_type = pattern['query_type']

            pattern_start = time.time()
            result, count = execute_pattern_with_enrichment(pattern_name, query_type)
            pattern_duration = time.time() - pattern_start
            total_pattern_time += pattern_duration

            if count >= 0:
                pattern_metrics[pattern_name] = count
            if count == 0 and result.empty:
                # Check if it was skipped due to error (count=0 and empty could be skip or just no matches)
                pass

            if not result.empty:
                all_results.append(result)

        # 6. Combine, dedup, write
        if all_results:
            print(f"\n🔄 Combining and deduplicating results...")
            combined = pd.concat(all_results, ignore_index=True)

            pre_dedup_count = len(combined)
            print(f"Pre-deduplication: {pre_dedup_count} total records")

            combined = combined.drop_duplicates(subset=['order_identifier'], keep='first')
            final_count = len(combined)

            print(f"Post-deduplication: {final_count} unique orders")
            if pre_dedup_count > final_count:
                print(f"⚠️ Removed {pre_dedup_count - final_count} duplicate records")

            write_start = time.time()
            success = write_to_sf_table(combined)
            write_time = time.time() - write_start

            if success:
                total_runtime = time.time() - total_start_time

                gct_count = len(combined[combined['query_type'] == 'GCT'])
                th_count = len(combined[combined['query_type'] == 'TH'])

                slack_message = format_slack_message(
                    pattern_metrics, skipped_patterns, final_count, gct_count, th_count,
                    total_runtime, adhoc_order_count
                )
                print(f"\n{slack_message}")

                with open("trader_alerts_slack_message.txt", "w") as f:
                    f.write(slack_message)

                print(f"\n✅ Ad hoc pipeline completed successfully!")
                print(
                    f"⏱️ Breakdown: AdHoc({adhoc_time:.1f}s) + Temp({temp_creation_time:.1f}s) "
                    f"+ Patterns({total_pattern_time:.1f}s) + Write({write_time:.1f}s) "
                    f"= Total({total_runtime:.1f}s)"
                )
            else:
                print("❌ Failed to write results")

        else:
            total_runtime = time.time() - total_start_time
            no_results_message = (
                f"🚨 Trader Alerts Results (AD HOC TASK)\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📋 Scoped to {adhoc_order_count} ad hoc orders\n"
                f"📂 Source: trader_tfa_review_frame_vw\n\n"
                f"✅ No pattern matches found for these orders\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Total: 0 orders\n"
                f"⏱️ Runtime: {total_runtime:.1f}s"
            )
            print(f"\n{no_results_message}")

            with open("trader_alerts_slack_message.txt", "w") as f:
                f.write(no_results_message)

    except Exception as e:
        total_runtime = time.time() - total_start_time
        error_message = (
            f"❌ Trader Hunting Pipeline (Ad Hoc) Failed\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Error: {str(e)}\n"
            f"⏱️ Runtime: {total_runtime:.1f}s"
        )
        print(f"\n{error_message}")

        with open("trader_alerts_slack_message.txt", "w") as f:
            f.write(error_message)

        raise

    finally:
        cleanup_temp_tables()


if __name__ == '__main__':
    main()