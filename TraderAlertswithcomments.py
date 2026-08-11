# trader_alerts_slack_format.py - Multi-pattern with Slack formatting
from ai.snowflake.compat.connection import sf
import pandas as pd
import re
import time
from snowflake.connector.pandas_tools import write_pandas


def get_active_patterns():
    """
    STEP 1: Read the registry table to get all Active pattern view names.
    Returns a list of dicts like [{'table_name': 'TRADER_AMR_Common_EM_DOM', 'query_type': 'TH'}, ...]
    Only patterns marked 'Active' are returned. Inactive patterns are completely skipped.
    query_type is either 'TH' (Trader Hunting = suspicious) or 'GCT' (Genuine Customer Test = safe).
    """
    query = """
            SELECT table_name, query_type
            FROM gbi_fraud_bap_db.ai_live_biz_app.trader_hunting_patterns
            WHERE activity = 'Active'
            ORDER BY query_type, table_name
            """
    # Execute the query against Snowflake, returns a pandas DataFrame
    df = sf.run_query(query)
    # Convert DataFrame rows to a list of dictionaries for easy iteration in main()
    return df.to_dict('records')


def get_trader_hunting_query(hunting_pattern):
    """
    STEP 2: For a given pattern name, call GET_DDL to retrieve the full
    CREATE VIEW statement as a raw text string.
    The pattern name IS the view name in Snowflake (e.g. 'AMR_gibberish_em_a1add_velocity').
    GET_DDL returns the entire DDL including CREATE OR REPLACE VIEW ... AS <actual SQL>.
    We need to strip the wrapper in the next step (transform_query) to get executable SQL.
    """
    # Build the GET_DDL call using the pattern name from the registry
    original_query = f"""
    select get_ddl('view', 'gbi_fraud_bap_db.ai_live_biz_app.{hunting_pattern}') as query
    """
    # Execute and get the DDL text back as a single-row DataFrame
    df = sf.run_query(original_query)
    # Return just the raw DDL string (first row, 'query' column)
    return df['query'][0]


def transform_query(query):
    """
    STEP 3: Take the raw DDL text from GET_DDL and extract ONLY the executable SQL.
    Input looks like:  'create or replace view FOO (COL1, COL2) AS SELECT ... FROM ... WHERE ...'
    Output looks like: 'SELECT ... FROM ... WHERE ...'
    This is what allows us to run the view's logic as a standalone query.
    """
    try:
        # Guard against empty/None input
        if not query:
            return None

        # Use regex to find the 'AS' keyword that separates the CREATE VIEW header from the SQL body.
        # \b = word boundary so we don't match 'AS' inside column names like 'CLASSIFIED_AS_FRAUD'.
        # re.DOTALL makes '.' match newlines so we capture multi-line SQL.
        # Everything after 'AS ' is captured in group(1).
        as_match = re.search(r'\bAS\s+(.*)', query, re.IGNORECASE | re.DOTALL)

        if not as_match:
            print("❌ Could not find AS clause in query")
            return None

        # Extract everything after 'AS' — this is the SQL body
        extracted = as_match.group(1).strip()

        # Some views wrap the SQL in parentheses: AS (SELECT ...).
        # Others don't: AS WITH cte AS (...) SELECT ...
        # If outer parens exist, strip them so the SQL is directly executable.
        if extracted.startswith('(') and extracted.endswith(')'):
            extracted = extracted[1:-1].strip()

        # Clean up any trailing semicolons that GET_DDL might include
        extracted = re.sub(r';+\s*$', '', extracted)

        # Some older views reference a legacy database path.
        # Replace it with the current path so the SQL runs correctly.
        transformed = re.sub(
            r"gbi_trader_semantic_db\.ai\.trader_orders",
            r"gbi_fraud_bap_db.ai_live_biz_app.trader_orders",
            extracted,
            flags=re.IGNORECASE
        )

        # Return the clean, executable SQL string
        return transformed

    except Exception as e:
        print(f"❌ Transform error: {e}")
        return None


def create_temp_table():
    """
    PERFORMANCE OPTIMIZATION: Create a temporary table with only the last 5 days
    of data from the full frame table. This way each pattern query scans a small
    table instead of the entire frame table. The temp table is used in two places:
      1. Inside the pattern SQL (we string-replace the table name)
      2. In the enrichment LEFT JOIN (Step 5)
    SELECT * means it inherits ALL columns dynamically — no hardcoding here.
    The temp table is dropped in cleanup_temp_table() in the finally block.
    """
    print("Creating temporary table...")

    # Drop any leftover temp table from a previous failed run
    drop_sql = "DROP TABLE IF EXISTS gbi_fraud_bap_db.ai_live_biz_app.trader_temp_frame"
    sf.run_query(drop_sql)

    # Create the temp table — SELECT * grabs all columns from the frame table.
    # Filter to last 5 days so pattern queries run fast.
    sql = """
    CREATE TEMP TABLE trader_temp_frame AS
    SELECT *
    FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame
    WHERE EVENT_TS >= current_date - 5
    """

    # Time the creation for performance reporting in the Slack message
    start_time = time.time()
    sf.run_query(sql)
    duration = time.time() - start_time
    print(f"Temp table created in {duration:.2f}s")
    return duration


def create_results_table():
    """
    Create the output table that analysts query for results.
    CREATE OR REPLACE means previous run's data is destroyed — historical
    data is preserved in the logging table (trader_TFA_SL_TH_logging).
    The column list here MUST exactly match what execute_pattern_with_enrichment()
    produces, otherwise write_pandas() will fail.
    """
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
    """
    STEPS 4 + 5 COMBINED: This is the core of the pipeline.
    For one pattern, it:
      - Extracts the view DDL (Step 2)
      - Strips the CREATE VIEW wrapper (Step 3)
      - Executes the SQL but ONLY keeps order_id/web_order_id (Step 4)
      - JOINs those order IDs back to the frame table for full enrichment (Step 5)

    The key insight: every view returns DIFFERENT columns (EMAIL_HANDLE, NM2EM_MATCH_SCORE, etc.)
    but we DISCARD all of them. We only use the view to IDENTIFY which orders are suspicious,
    then we re-enrich from the frame table so every pattern produces the SAME output schema.
    """
    try:
        print(f"  🚀 {pattern_name}: ", end="")

        # STEP 2: Get the full CREATE VIEW DDL text for this pattern
        query = get_trader_hunting_query(pattern_name)

        # STEP 3: Strip the CREATE VIEW wrapper, leaving just the executable SQL
        transformed_query = transform_query(query)

        # If regex parsing failed, skip this pattern
        if not transformed_query:
            print("Could not extract query")
            return pd.DataFrame(), 0

        # PERFORMANCE: Replace the full frame table reference with our temp table (last 5 days only).
        # The pattern's SQL was written against the full table — this swap makes it run faster.
        transformed_query = transformed_query.replace(
            'gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame',
            'trader_temp_frame'
        )

        # ACTION_LEVEL LOGIC: Determines how the order should be treated downstream.
        # GCT views have their own action_level column in their output — we pass it through.
        # TH views do NOT have action_level — we assign it based on pattern name.
        if query_type == 'GCT':
            # GCT: the view itself defines action_level, pull it from the subquery
            action_level_sql = "COALESCE(t.action_level, 'REVIEW')"
            # Include action_level in the inner SELECT so it's available to the outer query
            select_action_level = ", action_level"
        else:
            # TH: no action_level column exists in the view, assign based on name
            if 'AMR' in pattern_name.upper():
                # AMR patterns are high confidence — mark as HIGH_RISK
                action_level_sql = "'HIGH_RISK'"
            else:
                # All other TH patterns default to REVIEW
                action_level_sql = "'REVIEW'"
            # Empty string — don't try to SELECT action_level from TH views (it doesn't exist)
            select_action_level = ""

        # BUILD THE COMBINED STEP 4 + STEP 5 QUERY
        #
        # INNER SUBQUERY (aliased as "t") = STEP 4:
        #   Runs the full pattern SQL (could have CTEs, JOINs, complex WHERE clauses).
        #   DISCARDS all view-specific columns (EMAIL_HANDLE, NM2EM_MATCH_SCORE, etc.).
        #   ONLY extracts: order_identifier = COALESCE(web_order_id, order_id).
        #   Filters out any rows where both order IDs are NULL.
        #
        # OUTER QUERY = STEP 5:
        #   Takes order_identifiers from the inner subquery.
        #   LEFT JOINs to trader_temp_frame to pull the full 50+ column enrichment.
        #   Adds metadata: hunting_pattern name, query_type, ACTION_LEVEL, timestamp.
        #   LEFT JOIN (not INNER) so orders still appear even if temp table match fails.
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
            /* STEP 4 — INNER SUBQUERY: Run the pattern, keep ONLY order IDs.
               The pattern SQL runs here in full (all its CTEs, JOINs, WHERE filters).
               But we wrap it and only pull out the order identifier.
               All view-specific columns are thrown away at this boundary. */
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
        /* STEP 5 — ENRICHMENT JOIN: Take order IDs from Step 4, join back to
           the temp frame table to get the full standardized set of 50+ columns.
           This is why all patterns produce identical output schemas regardless
           of what columns the view itself returns. */
        LEFT JOIN trader_temp_frame tf
            ON t.order_identifier = COALESCE(tf.WEB_ORDER_ID, tf.ORDER_ID::VARCHAR)
        """

        # Execute the combined query and measure how long it takes
        start_time = time.time()
        result = sf.run_query(enriched_query)
        duration = time.time() - start_time

        # Count results — empty DataFrame means 0 orders matched this pattern
        row_count = len(result) if not result.empty else 0
        print(f"{row_count} orders in {duration:.3f}s")

        return result, row_count

    except Exception as e:
        # SILENT FAILURE: If this pattern breaks (bad column reference, upstream table changed, etc.)
        # we catch the error, print it, and return empty. The pipeline continues to the next pattern.
        # This pattern will show 0 orders in the Slack message but there's no explicit "BROKEN" alert.
        print(f"ERROR - {str(e)}")
        return pd.DataFrame(), 0


def write_to_sf_table(combined_results):
    """
    STEP 6: Write the combined, deduplicated DataFrame to the Snowflake results table.
    Uses write_pandas which internally does PUT (stage the data) + COPY INTO (load it).
    Requires explicit COMMIT because write_pandas doesn't always auto-commit.
    """
    # Guard against writing an empty DataFrame
    if combined_results.empty:
        print("❌ No data to write - DataFrame is empty")
        return False

    try:
        print(f"🚀 Writing {len(combined_results)} rows to results table...")

        # Clear any existing data — safety measure since we already did CREATE OR REPLACE
        sf.run_query("TRUNCATE TABLE gbi_fraud_bap_db.ai_live_biz_app.Trader_Automated_Queries_Results")

        # Reset the DataFrame index so write_pandas doesn't choke on non-sequential indices
        # (can happen after pd.concat + drop_duplicates)
        combined_results = combined_results.reset_index(drop=True)

        # Get the raw Snowflake connector connection — write_pandas needs the raw connector,
        # not the SQLAlchemy wrapper that sf uses internally
        conn = sf._backend._engine.connect()

        # write_pandas: bulk-loads the DataFrame into the target table
        # auto_create_table=False: we already created the table with exact schema
        # use_logical_type=True: handles datetime columns properly (avoids pandas warnings)
        # quote_identifiers=False: column names match exactly, no quoting needed
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

        # EXPLICIT COMMIT — ensures data is actually persisted to the table
        conn.connection.commit()
        # Close the connection to release resources
        conn.close()

        print(f"✅ Successfully wrote {len(combined_results)} rows")
        return True

    except Exception as e:
        print(f"❌ Write failed: {e}")
        return False


def cleanup_temp_table():
    """
    Drop the temp table. Called in the finally block so it runs
    whether the pipeline succeeds or fails. Temp tables are session-scoped
    and would auto-drop, but explicit cleanup is good practice.
    """
    sf.run_query("DROP TABLE IF EXISTS trader_temp_frame")


def format_slack_message(pattern_metrics, total_count, gct_count, th_count, total_runtime):
    """
    STEP 7a: Build a human-readable Slack message summarizing the pipeline run.
    Groups patterns into GCT (genuine) and TH (hunting) sections.
    Shows per-pattern order counts, totals, and runtime.
    Note: per-pattern counts are PRE-dedup so they may sum to more than total_count.
    """
    # Header
    message = "🚨 Trader Alerts Results\n"
    message += "━━━━━━━━━━━━━━━━━━━━━━\n\n"

    # Split pattern_metrics into GCT patterns (start with trader_genuine or trader_good)
    gct_patterns = {k: v for k, v in pattern_metrics.items() if
                    k.startswith('trader_genuine') or k.startswith('trader_good')}
    # Show GCT section if any exist
    if gct_patterns:
        message += "✅ Genuine Customer Queries\n"
        for pattern, count in gct_patterns.items():
            if count > 0:  # Only show patterns that actually found orders
                message += f"{pattern}: {count} orders\n"
        message += "\n"

    # Everything else is a TH (Trader Hunting) pattern
    th_patterns = {k: v for k, v in pattern_metrics.items() if k not in gct_patterns}
    if th_patterns:
        message += "🔍 Trader Hunting Queries\n"
        for pattern, count in th_patterns.items():
            if count > 0:
                message += f"{pattern}: {count} orders\n"
        message += "\n"

    # Footer: totals and the query analysts can run to see full results
    message += "━━━━━━━━━━━━━━━━━━━━━━\n"
    message += f"📊 Total: {total_count} orders\n"
    message += f"⏱️ Runtime: {total_runtime:.1f}s\n\n"
    message += f"✅ GCT: {gct_count} | 🔍 TH: {th_count}\n\n"
    message += "SELECT * FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_Automated_Queries_Results"

    return message


def load_to_action_history():
    """
    STEP 7b: Append this run's results to the permanent logging table.
    The results table gets wiped every run (CREATE OR REPLACE), so this
    logging table is the only place that keeps a historical record of
    every order flagged by every pattern on every run.
    """
    print("📋 Saving order IDs to logging table...")
    start_time = time.time()

    # INSERT INTO ... SELECT: copies current results into the append-only log.
    # CURRENT_TIMESTAMP() becomes the RUN_TS so we know when this batch ran.
    insert_query = """
                   INSERT INTO gbi_fraud_bap_db.ai_live_biz_app.trader_TFA_SL_TH_logging
                   SELECT CURRENT_TIMESTAMP(),
                          ORDER_ID,
                          WEB_ORDER_ID,
                          HUNTING_PATTERN,
                          QUERY_TYPE
                   FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_Automated_Queries_Results
                   """

    sf.run_query(insert_query)

    # Verify: count how many rows we just inserted (anything in the last 5 minutes)
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
    """
    ORCHESTRATOR: Runs all 7 steps in sequence.
    Step 1: Get active patterns from registry
    Step 2-5: For each pattern → GET_DDL → strip wrapper → execute → enrich
    Step 6: Combine all results, deduplicate, write to Snowflake
    Step 7: Log to history table, format and save Slack message
    """
    print("🚀 TRADER HUNTING PIPELINE - SLACK FORMAT")
    print("=" * 50)

    # Start the overall timer — reported in Slack message
    total_start_time = time.time()

    try:
        # ── STEP 1: Get the list of active patterns from the registry table ──
        # Returns: [{'table_name': 'TRADER_AMR_Common_EM_DOM', 'query_type': 'TH'}, ...]
        patterns = get_active_patterns()
        print(f"Found {len(patterns)} active patterns")

        # Create the output table with hardcoded schema (CREATE OR REPLACE wipes previous run)
        create_results_table()

        # Create temp table with last 5 days of frame data for faster pattern execution
        temp_creation_time = create_temp_table()

        # ── STEPS 2-5: Loop through each active pattern ──
        print("\nExecuting patterns:")
        all_results = []          # Collects DataFrames from each pattern
        pattern_metrics = {}      # {pattern_name: count} for the Slack message
        total_pattern_time = 0    # Cumulative time across all patterns

        for pattern in patterns:
            pattern_name = pattern['table_name']   # The Snowflake view name
            query_type = pattern['query_type']      # 'TH' or 'GCT'

            # Time each pattern individually
            pattern_start = time.time()
            # This single call does Steps 2→3→4→5:
            #   2. GET_DDL to retrieve the view definition
            #   3. Regex to strip CREATE VIEW and get executable SQL
            #   4. Execute SQL, extract only order_id/web_order_id
            #   5. LEFT JOIN to frame table for 50+ column enrichment
            result, count = execute_pattern_with_enrichment(pattern_name, query_type)
            pattern_duration = time.time() - pattern_start
            total_pattern_time += pattern_duration

            # Track this pattern's count for the Slack summary
            pattern_metrics[pattern_name] = count

            # Only add to combined results if this pattern found orders
            if not result.empty:
                all_results.append(result)

        # ── STEP 6: Combine all pattern results and deduplicate ──
        if all_results:
            print(f"\n🔄 Combining and deduplicating results...")

            # Stack all pattern DataFrames into one big DataFrame
            # Same order might appear in multiple patterns at this point
            combined = pd.concat(all_results, ignore_index=True)

            pre_dedup_count = len(combined)
            print(f"Pre-deduplication: {pre_dedup_count} total records")

            # Remove duplicate orders — keep the first pattern that flagged each order.
            # "First" is determined by the loop order (sorted by query_type, then table_name).
            combined = combined.drop_duplicates(subset=['order_identifier'], keep='first')
            final_count = len(combined)

            print(f"Post-deduplication: {final_count} unique orders")
            if pre_dedup_count > final_count:
                print(f"⚠️ Removed {pre_dedup_count - final_count} duplicate records")

            # Write the deduplicated results to the Snowflake output table
            write_start = time.time()
            success = write_to_sf_table(combined)
            write_time = time.time() - write_start

            if success:
                # ── STEP 7: Log to history + Slack message ──

                # 7b: Append order IDs to the permanent logging table
                load_to_action_history()

                # Calculate final metrics for the Slack message
                total_runtime = time.time() - total_start_time

                # Count how many orders are GCT vs TH in the final output
                gct_count = len(combined[combined['query_type'] == 'GCT'])
                th_count = len(combined[combined['query_type'] == 'TH'])

                # 7a: Format the Slack summary with per-pattern counts and totals
                slack_message = format_slack_message(pattern_metrics, final_count, gct_count, th_count, total_runtime)
                print(f"\n{slack_message}")

                # Save message to a text file — Jenkins post-build step or webhook picks this up
                with open("trader_alerts_slack_message.txt", "w") as f:
                    f.write(slack_message)

                # Print performance breakdown
                print(f"\n✅ Pipeline completed successfully!")
                print(
                    f"⏱️ Breakdown: Temp({temp_creation_time:.1f}s) + Patterns({total_pattern_time:.1f}s) + Write({write_time:.1f}s) = Total({total_runtime:.1f}s)")

            else:
                print("❌ Failed to write results")

        else:
            # No patterns returned any results — write a "clean" Slack message
            total_runtime = time.time() - total_start_time
            no_results_message = f"🚨 Trader Alerts Results\n━━━━━━━━━━━━━━━━━━━━━━\n\n✅ No suspicious orders detected\n\n━━━━━━━━━━━━━━━━━━━━━━\n📊 Total: 0 orders\n⏱️ Runtime: {total_runtime:.1f}s"
            print(f"\n{no_results_message}")

            with open("trader_alerts_slack_message.txt", "w") as f:
                f.write(no_results_message)

    except Exception as e:
        # PIPELINE-LEVEL FAILURE: Individual pattern errors are caught in execute_pattern_with_enrichment.
        # If we land here, something systemic broke (registry query, temp table, write_pandas, etc.)
        total_runtime = time.time() - total_start_time
        error_message = f"❌ Trader Hunting Pipeline Failed\n━━━━━━━━━━━━━━━━━━━━━━\n\nError: {str(e)}\n⏱️ Runtime: {total_runtime:.1f}s"
        print(f"\n{error_message}")

        with open("trader_alerts_slack_message.txt", "w") as f:
            f.write(error_message)

        # Re-raise so Jenkins marks the build as FAILED
        raise

    finally:
        # ALWAYS clean up the temp table — runs on success, failure, or exception
        cleanup_temp_table()


if __name__ == '__main__':
    main()
