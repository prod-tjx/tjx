# trader_alerts_adhoc_task.py - TJH pipeline scoped to ad hoc thresholds orders
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


def transform_query(query):
    """Simple universal query transformer that works for all patterns"""
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
    """Create temp table of ad hoc order_ids to scope the run"""
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
    """Create temp table filtered to only ad hoc orders"""
    print("Creating temporary frame table (filtered to ad hoc orders)...")

    sf.run_query("DROP TABLE IF EXISTS trader_temp_frame")

    # Only pull rows whose ORDER_ID is in our ad hoc set
    sql = """
    CREATE TEMP TABLE trader_temp_frame AS
    SELECT tf.*
    FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame tf
    INNER JOIN adhoc_task_orders ao
        ON tf.ORDER_ID = ao.order_id
    """

    start_time = time.time()
    sf.run_query(sql)
    duration = time.time() - start_time

    count_df = sf.run_query("SELECT COUNT(*) AS cnt FROM trader_temp_frame")
    frame_count = count_df['cnt'][0]
    print(f"Temp frame created: {frame_count} rows in {duration:.2f}s")
    return duration


def create_results_table():
    """Create results table with all necessary columns"""
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
    """Execute a single pattern, then filter to only ad hoc orders"""
    try:
        print(f"  🚀 {pattern_name}: ", end="")

        query = get_trader_hunting_query(pattern_name)
        transformed_query = transform_query(query)

        if not transformed_query:
            print("Could not extract query")
            return pd.DataFrame(), 0

        # Replace the main table with temp table
        transformed_query = transformed_query.replace(
            'gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame',
            'trader_temp_frame'
        )

        # ACTION_LEVEL logic (same as original)
        if query_type == 'GCT':
            action_level_sql = "COALESCE(t.action_level, 'REVIEW')"
            select_action_level = ", action_level"
        else:
            if 'AMR' in pattern_name.upper():
                action_level_sql = "'HIGH_RISK'"
            else:
                action_level_sql = "'REVIEW'"
            select_action_level = ""

        # Build enriched query — INNER JOIN to adhoc_task_orders
        # ensures only our scoped orders come through
        enriched_query = f"""
        SELECT 
            t.order_identifier,
            '{pattern_name}' AS hunting_pattern,
            '{query_type}' AS query_type,
            {action_level_sql} AS ACTION_LEVEL,
            tf.ORDER_ID, 
            tf.WEB_ORDER_ID,
            tf.EVENT_TS,
            tf.COMMIT_CD,
            tf.COMMIT_DT,
            tf.SALES_ORG,
            tf.PO_TYPE,
            tf.SALES_DISTRICT,
            tf.DISCOUNT_STORE_TYPE,
            tf.SLA,
            tf.EXCEPTION_CD,
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
            tf.A1_PHONE,
            tf.LATEST_A1_NM,
            tf.LATEST_A1_EM,
            tf.LASTEST_A1_EM_DOM,
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
            tf.IP_BIN_COUNTRY,
            tf.PAYMENT_TYPES,
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
        -- Enrich with frame data
        LEFT JOIN trader_temp_frame tf
            ON t.order_identifier = COALESCE(tf.WEB_ORDER_ID, tf.ORDER_ID::VARCHAR)
        -- *** SCOPE TO AD HOC ORDERS ONLY ***
        INNER JOIN adhoc_task_orders ao
            ON COALESCE(tf.ORDER_ID::VARCHAR, t.order_identifier) = ao.order_id::VARCHAR
        """

        start_time = time.time()
        result = sf.run_query(enriched_query)
        duration = time.time() - start_time

        row_count = len(result) if not result.empty else 0
        print(f"{row_count} orders in {duration:.3f}s")

        return result, row_count

    except Exception as e:
        print(f"ERROR - {str(e)}")
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


def format_slack_message(pattern_metrics, total_count, gct_count, th_count, total_runtime, adhoc_order_count):
    """Format the Slack message — ad hoc version"""

    message = "🚨 Trader Alerts Results (AD HOC TASK)\n"
    message += "━━━━━━━━━━━━━━━━━━━━━━\n"
    message += f"📋 Scoped to {adhoc_order_count} ad hoc orders (US / SFS / 2026-02-14–15)\n\n"

    # GCT section
    gct_patterns = {k: v for k, v in pattern_metrics.items() if
                    k.startswith('trader_genuine') or k.startswith('trader_good')}
    if gct_patterns:
        message += "✅ Genuine Customer Queries\n"
        for pattern, count in gct_patterns.items():
            if count > 0:
                message += f"  {pattern}: {count} orders\n"
        message += "\n"

    # TH section
    th_patterns = {k: v for k, v in pattern_metrics.items() if k not in gct_patterns}
    if th_patterns:
        message += "🔍 Trader Hunting Queries\n"
        for pattern, count in th_patterns.items():
            if count > 0:
                message += f"  {pattern}: {count} orders\n"
        message += "\n"

    message += "━━━━━━━━━━━━━━━━━━━━━━\n"
    message += f"📊 Total: {total_count} orders\n"
    message += f"⏱️ Runtime: {total_runtime:.1f}s\n\n"
    message += f"✅ GCT: {gct_count} | 🔍 TH: {th_count}\n\n"
    message += "SELECT * FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_Automated_Queries_Results"

    return message


def main():
    print("🚀 TRADER HUNTING PIPELINE - AD HOC TASK")
    print("=" * 50)

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

        # 4. Create temp frame (filtered to ad hoc orders only)
        temp_creation_time = create_temp_table()

        # 5. Execute each pattern against the scoped data
        print("\nExecuting patterns against ad hoc orders:")
        all_results = []
        pattern_metrics = {}
        total_pattern_time = 0

        for pattern in patterns:
            pattern_name = pattern['table_name']
            query_type = pattern['query_type']

            pattern_start = time.time()
            result, count = execute_pattern_with_enrichment(pattern_name, query_type)
            pattern_duration = time.time() - pattern_start
            total_pattern_time += pattern_duration

            pattern_metrics[pattern_name] = count

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
                    pattern_metrics, final_count, gct_count, th_count,
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
                f"📋 Scoped to {adhoc_order_count} ad hoc orders\n\n"
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