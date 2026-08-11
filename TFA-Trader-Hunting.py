# trader_alerts_slack_format.py - Multi-pattern with Slack formatting
from ai.snowflake.compat.connection import sf
import pandas as pd
import re
import time
from snowflake.connector.pandas_tools import write_pandas


def get_active_patterns():
    """Get active patterns with query types"""
    query = """
            SELECT table_name, query_type
            FROM gbi_fraud_bap_db.ai_live_biz_app.trader_hunting_patterns
            WHERE activity = 'Active'
            ORDER BY query_type, table_name \
            """
    df = sf.run_query(query)
    return df.to_dict('records')


def get_trader_hunting_query(hunting_pattern):
    """Get DDL for trader hunting view"""
    original_query = f"""
    select get_ddl('view', 'gbi_fraud_bap_db.ai_live_biz_app.{hunting_pattern}') as query
    """
    df = sf.run_query(original_query)
    return df['query'][0]


def transform_query(query):
    """Simple universal query transformer that works for all patterns"""
    try:
        if not query:
            return None

        # Find the "AS" keyword and extract everything after it
        # This works for both "AS (SELECT..." and "AS WITH..." formats
        as_match = re.search(r'\bAS\s+(.*)', query, re.IGNORECASE | re.DOTALL)

        if not as_match:
            print("❌ Could not find AS clause in query")
            return None

        # Get everything after "AS"
        extracted = as_match.group(1).strip()

        # Remove outer parentheses if they exist (for "AS (SELECT...)" format)
        if extracted.startswith('(') and extracted.endswith(')'):
            extracted = extracted[1:-1].strip()

        # Remove trailing semicolon
        extracted = re.sub(r';+\s*$', '', extracted)

        # Replace table references for temp table
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


def create_temp_table():
    """Create temp table for faster pattern execution"""
    print("Creating temporary table...")

    # Drop existing temp table if it exists
    drop_sql = "DROP TABLE IF EXISTS gbi_fraud_bap_db.ai_live_biz_app.trader_temp_frame"
    sf.run_query(drop_sql)

    # Create temp table with recent data
    sql = """
    CREATE TEMP TABLE trader_temp_frame AS
    SELECT *
    FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame
    WHERE EVENT_TS >= current_date - 5
    """

    start_time = time.time()
    sf.run_query(sql)
    duration = time.time() - start_time
    print(f"Temp table created in {duration:.2f}s")
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
    """Execute a single pattern with full enrichment - FIXED ACTION_LEVEL"""
    try:
        print(f"  🚀 {pattern_name}: ", end="")

        # Get and transform the pattern query
        query = get_trader_hunting_query(pattern_name)
        transformed_query = transform_query(query)

        if not transformed_query:
            print("Could not extract query")
            return pd.DataFrame(), 0

        # Replace the main table with temp table for faster execution
        transformed_query = transformed_query.replace(
            'gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame',
            'trader_temp_frame'
        )

        # Build enriched query - COMPLETELY DIFFERENT ACTION_LEVEL LOGIC
        if query_type == 'GCT':
            # For GCT queries - use their action_level
            action_level_sql = "COALESCE(t.action_level, 'REVIEW')"
            select_action_level = ", action_level"
        else:
            # For TH queries - hardcoded logic, NO t.action_level reference
            if 'AMR' in pattern_name.upper():
                action_level_sql = "'HIGH_RISK'"
            else:
                action_level_sql = "'REVIEW'"
            select_action_level = ""

        enriched_query = f"""
        SELECT 
            t.order_identifier,
            '{pattern_name}' as hunting_pattern,
            '{query_type}' as query_type,
            {action_level_sql} as ACTION_LEVEL,
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
            CURRENT_TIMESTAMP as CREATED_TS
        FROM (
            SELECT 
                COALESCE(
                    TRY_CAST(web_order_id AS VARCHAR), 
                    TRY_CAST(order_id AS VARCHAR),
                    'UNKNOWN'
                ) as order_identifier
                {select_action_level}
            FROM ({transformed_query})
            WHERE COALESCE(
                TRY_CAST(web_order_id AS VARCHAR), 
                TRY_CAST(order_id AS VARCHAR)
            ) IS NOT NULL
        ) t
        LEFT JOIN trader_temp_frame tf
            ON t.order_identifier = COALESCE(tf.WEB_ORDER_ID, tf.ORDER_ID::VARCHAR)
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

        # Clear existing data first
        sf.run_query("TRUNCATE TABLE gbi_fraud_bap_db.ai_live_biz_app.Trader_Automated_Queries_Results")

        # Fix pandas warnings
        combined_results = combined_results.reset_index(drop=True)

        # Get connection and use explicit commit
        conn = sf._backend._engine.connect()

        result = write_pandas(
            conn=conn.connection,
            df=combined_results,
            database='gbi_fraud_bap_db',
            schema='ai_live_biz_app',
            table_name='Trader_Automated_Queries_Results',
            quote_identifiers=False,
            auto_create_table=False,
            use_logical_type=True  # Fix datetime warnings
        )

        # EXPLICIT COMMIT
        conn.connection.commit()
        conn.close()

        print(f"✅ Successfully wrote {len(combined_results)} rows")
        return True

    except Exception as e:
        print(f"❌ Write failed: {e}")
        return False


def cleanup_temp_table():
    """Clean up temporary table"""
    sf.run_query("DROP TABLE IF EXISTS trader_temp_frame")


def format_slack_message(pattern_metrics, total_count, gct_count, th_count, total_runtime):
    """Format the Slack message like the TFA Trader Notification"""

    # Header with emojis
    message = "🚨 Trader Alerts Results\n"
    message += "━━━━━━━━━━━━━━━━━━━━━━\n\n"

    # Genuine Customer Queries section
    gct_patterns = {k: v for k, v in pattern_metrics.items() if
                    k.startswith('trader_genuine') or k.startswith('trader_good')}
    if gct_patterns:
        message += "✅ Genuine Customer Queries\n"
        for pattern, count in gct_patterns.items():
            if count > 0:
                message += f"{pattern}: {count} orders\n"
        message += "\n"

    # Trader Hunting Queries section
    th_patterns = {k: v for k, v in pattern_metrics.items() if k not in gct_patterns}
    if th_patterns:
        message += "🔍 Trader Hunting Queries\n"
        for pattern, count in th_patterns.items():
            if count > 0:
                message += f"{pattern}: {count} orders\n"
        message += "\n"

    # Footer with totals and runtime
    message += "━━━━━━━━━━━━━━━━━━━━━━\n"
    message += f"📊 Total: {total_count} orders\n"
    message += f"⏱️ Runtime: {total_runtime:.1f}s\n\n"
    message += f"✅ GCT: {gct_count} | 🔍 TH: {th_count}\n\n"
    message += "SELECT * FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_Automated_Queries_Results"

    return message


def load_to_action_history():
    """Append current run's order IDs to logging table"""

    print("📋 Saving order IDs to logging table...")
    start_time = time.time()

    insert_query = """
                   INSERT INTO gbi_fraud_bap_db.ai_live_biz_app.trader_TFA_SL_TH_logging
                   SELECT CURRENT_TIMESTAMP(), \
                          ORDER_ID, \
                          WEB_ORDER_ID, \
                          HUNTING_PATTERN, \
                          QUERY_TYPE
                   FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_Automated_Queries_Results \
                   """

    sf.run_query(insert_query)

    count_df = sf.run_query("""
                            SELECT COUNT(*) AS cnt
                            FROM gbi_fraud_bap_db.ai_live_biz_app.trader_TFA_SL_TH_logging
                            WHERE RUN_TS >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
                            """)
    row_count = count_df['cnt'][0] if not count_df.empty else 0

    duration = time.time() - start_time
    print(f"✅ Saved {row_count} order IDs to logging table in {duration:.2f}s")
    return row_count

def main():
    print("🚀 TRADER HUNTING PIPELINE - SLACK FORMAT")
    print("=" * 50)

    # Start total timer
    total_start_time = time.time()

    try:
        # Get active patterns
        patterns = get_active_patterns()
        print(f"Found {len(patterns)} active patterns")

        # Create results table
        create_results_table()

        # Create temp table for faster execution
        temp_creation_time = create_temp_table()

        # Execute patterns
        print("\nExecuting patterns:")
        all_results = []
        pattern_metrics = {}
        total_pattern_time = 0

        for pattern in patterns:
            pattern_name = pattern['table_name']
            query_type = pattern['query_type']

            # Time each pattern
            pattern_start = time.time()
            result, count = execute_pattern_with_enrichment(pattern_name, query_type)
            pattern_duration = time.time() - pattern_start
            total_pattern_time += pattern_duration

            pattern_metrics[pattern_name] = count

            if not result.empty:
                all_results.append(result)

        # Combine and deduplicate results
        if all_results:
            print(f"\n🔄 Combining and deduplicating results...")
            combined = pd.concat(all_results, ignore_index=True)

            # Show pre-dedup count
            pre_dedup_count = len(combined)
            print(f"Pre-deduplication: {pre_dedup_count} total records")

            # Deduplicate by order_identifier, keeping first occurrence
            combined = combined.drop_duplicates(subset=['order_identifier'], keep='first')
            final_count = len(combined)

            print(f"Post-deduplication: {final_count} unique orders")
            if pre_dedup_count > final_count:
                print(f"⚠️ Removed {pre_dedup_count - final_count} duplicate records")

            # Write results with timing
            write_start = time.time()
            success = write_to_sf_table(combined)
            write_time = time.time() - write_start

            if success:
                load_to_action_history()

                # Calculate total runtime
                total_runtime = time.time() - total_start_time

                # Calculate counts by query type
                gct_count = len(combined[combined['query_type'] == 'GCT'])
                th_count = len(combined[combined['query_type'] == 'TH'])

                # Format and print Slack message with runtime
                slack_message = format_slack_message(pattern_metrics, final_count, gct_count, th_count, total_runtime)
                print(f"\n{slack_message}")

                # Save to file for webhook
                with open("trader_alerts_slack_message.txt", "w") as f:
                    f.write(slack_message)

                print(f"\n✅ Pipeline completed successfully!")
                print(
                    f"⏱️ Breakdown: Temp({temp_creation_time:.1f}s) + Patterns({total_pattern_time:.1f}s) + Write({write_time:.1f}s) = Total({total_runtime:.1f}s)")

            else:
                print("❌ Failed to write results")

        else:
            # No results message with runtime
            total_runtime = time.time() - total_start_time
            no_results_message = f"🚨 Trader Alerts Results\n━━━━━━━━━━━━━━━━━━━━━━\n\n✅ No suspicious orders detected\n\n━━━━━━━━━━━━━━━━━━━━━━\n📊 Total: 0 orders\n⏱️ Runtime: {total_runtime:.1f}s"
            print(f"\n{no_results_message}")

            with open("trader_alerts_slack_message.txt", "w") as f:
                f.write(no_results_message)

    except Exception as e:
        # Error message with runtime
        total_runtime = time.time() - total_start_time
        error_message = f"❌ Trader Hunting Pipeline Failed\n━━━━━━━━━━━━━━━━━━━━━━\n\nError: {str(e)}\n⏱️ Runtime: {total_runtime:.1f}s"
        print(f"\n{error_message}")

        with open("trader_alerts_slack_message.txt", "w") as f:
            f.write(error_message)

        raise

    finally:
        # Always cleanup temp table
        cleanup_temp_table()


if __name__ == '__main__':
    main()
