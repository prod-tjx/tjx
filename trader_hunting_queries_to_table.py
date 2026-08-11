"""Builds trader hunting table — standalone version
   (lives in ai-sds-fulfillment-tfa)

DRI: tjx@apple.com
Jenkins Job Name: tfa_trader_hunting_queries_to_table
Repo path: tfa-toolkit/src/ai_sds_fulfillment_tfa/table_automation/AutoCT_Pipeline
Active queries: tfa-toolkit/src/ai_sds_fulfillment_tfa/table_automation/AutoCT_Pipeline/active_queries

Reads CREATE OR REPLACE VIEW *.sql files from --query_files_dir, extracts the
SELECT body, runs each against an FF-excluded temp frame, enriches with full
column set, dedupes across patterns, writes to Trader_Automated_Queries_Results,
appends to logging table, and emits a Slack-formatted summary file for Jenkins.
"""
import argparse
import glob
import os
import os.path
import re
import sys
import time

import pandas as pd
from ai.snowflake.compat.connection import sf
from snowflake.connector.pandas_tools import write_pandas


# -------------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------------
# Resolve paths relative to this file so the script works regardless of CWD
# (Jenkins, local dev, IDE run configs, etc).
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_QUERY_FILES_DIR = os.path.join(SCRIPT_DIR, "active_queries")
DEFAULT_SLACK_FILE      = os.path.join(SCRIPT_DIR, "trader_alerts_slack_message.txt")


# -------------------------------------------------------------------------
# Config
# -------------------------------------------------------------------------
WORKING_SCHEMA = "gbi_fraud_bap_db.ai_live_biz_app"
RESULTS_TABLE  = f"{WORKING_SCHEMA}.Trader_Automated_Queries_Results"
LOGGING_TABLE  = f"{WORKING_SCHEMA}.trader_TFA_SL_TH_logging"
FRAME_TABLE    = f"{WORKING_SCHEMA}.Trader_TFA_traderhunt_frame"
FF_TABLE       = f"{WORKING_SCHEMA}.tfa_aso_freight_forwarder_address_log"
TEMP_FRAME     = "trader_temp_frame"

LIVE_ROLE     = "GBI_FRAUD_BAP_DB_AI_LIVE_BIZ_APP_MAIN_ROLE"
RESEARCH_ROLE = "GBI_FRAUD_BAP_DB_AI_RESEARCH_BIZ_APP_MAIN_ROLE"


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------
def parse_exclusion_list(value):
    """Accepts either a path to a file (one query per line) OR a comma/space
       separated string straight from the Jenkins parameter."""
    if not value:
        return []
    if os.path.isfile(value):
        with open(value) as f:
            items = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
    else:
        items = [s.strip() for s in re.split(r"[,\s]+", value) if s.strip()]
    return items


def pattern_name_from_path(path):
    return os.path.splitext(os.path.basename(path))[0]


def infer_query_type(pattern_name):
    """GCT for genuine/good-product files; everything else is TH."""
    upper = pattern_name.upper()
    if "GENUINE" in upper or "GOOD_PRODUCT" in upper:
        return "GCT"
    return "TH"


def read_query_type_override(sql_text):
    """Optional per-file override:  -- query_type: GCT"""
    m = re.search(r"--\s*query_type\s*:\s*(GCT|TH)\b", sql_text, re.IGNORECASE)
    return m.group(1).upper() if m else None


def extract_select_from_ddl(sql_text):
    """Strip CREATE OR REPLACE VIEW ... AS wrapper, return inner SELECT.
       Mirrors the prod transform_query() logic."""
    if not sql_text:
        return None

    # Drop comments so they don't confuse the AS regex
    cleaned = re.sub(r"--[^\n]*", "", sql_text)
    cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)

    m = re.search(r"\bAS\b(.*)", cleaned, re.IGNORECASE | re.DOTALL)
    if not m:
        return None

    body = m.group(1).strip()
    if body.startswith("(") and body.endswith(")"):
        body = body[1:-1].strip()
    body = re.sub(r";+\s*$", "", body)

    # Source-table swaps
    body = re.sub(
        r"gbi_trader_semantic_db\.ai\.trader_orders",
        "gbi_fraud_bap_db.ai_live_biz_app.trader_orders",
        body, flags=re.IGNORECASE,
    )
    body = re.sub(re.escape(FRAME_TABLE), TEMP_FRAME, body, flags=re.IGNORECASE)
    return body


# -------------------------------------------------------------------------
# Snowflake setup
# -------------------------------------------------------------------------
def set_role(table_schema):
    if "live" in table_schema.lower():
        sf.run_query(f"USE ROLE {LIVE_ROLE}")
        print("\nUSING LIVE\n")
    else:
        sf.run_query(f"USE ROLE {RESEARCH_ROLE}")
        print("\nUSING RESEARCH\n")


def create_results_table():
    sql = f"""
    CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
        ORDER_ID                   VARCHAR(16777216),
        WEB_ORDER_ID               VARCHAR(16777216),
        hunting_pattern            VARCHAR(255),
        query_type                 VARCHAR(10),
        ACTION_LEVEL               VARCHAR(50),
        EVENT_TS                   DATE,
        COMMIT_CD                  VARCHAR(4),
        COMMIT_DT                  DATE,
        SALES_ORG                  VARCHAR(16777216),
        PO_TYPE                    VARCHAR(16777216),
        SALES_DISTRICT             VARCHAR(16777216),
        DISCOUNT_STORE_TYPE        VARCHAR(13),
        SLA                        VARCHAR(16777216),
        EXCEPTION_CD               VARCHAR(16777216),
        B2_NM                      VARCHAR(16777216),
        B2_EM                      VARCHAR(16777216),
        B2_EM_DOM                  VARCHAR(16777216),
        ACCT_AGE                   VARCHAR(16777216),
        B2_ADD                     VARCHAR(16777216),
        B2_CITY                    VARCHAR(16777216),
        B2_DISTRICT                VARCHAR(16777216),
        B2_STATE                   VARCHAR(16777216),
        B2_ZIP                     VARCHAR(16777216),
        B2_PH                      VARCHAR(16777216),
        A1_NM                      VARCHAR(16777216),
        A1_EM                      VARCHAR(16777216),
        A1_EM_DOM                  VARCHAR(16777216),
        A1_ADD                     VARCHAR(16777216),
        A1_CITY                    VARCHAR(16777216),
        A1_DISTRICT                VARCHAR(16777216),
        A1_STATE                   VARCHAR(16777216),
        A1_ZIP                     VARCHAR(16777216),
        A1_PHONE                   VARCHAR(16777216),
        LATEST_A1_NM               VARCHAR(16777216),
        LATEST_A1_EM               VARCHAR(16777216),
        LASTEST_A1_EM_DOM          VARCHAR(16777216),
        LATEST_A1_ADD              VARCHAR(16777216),
        LATEST_A1_STATE            VARCHAR(16777216),
        LATEST_A1_ZIP              VARCHAR(16777216),
        LATEST_BOPIS3_NM           VARIANT,
        LATEST_BOPIS3_EM           VARIANT,
        PROD_DESC                  VARCHAR(16777216),
        TOTAL_VALUE                NUMBER(38,2),
        IP_ADD                     VARCHAR(16777216),
        IP_CLASS                   VARCHAR(16777216),
        IP_COMPANY                 VARCHAR(16777216),
        IP_COUNTRY                 VARCHAR(16777216),
        IP_BIN_COUNTRY             VARCHAR(16777216),
        PAYMENT_TYPES              VARCHAR(16777216),
        CARD_BIN                   VARCHAR(16777216),
        BIN_COUNTRY                VARCHAR(16777216),
        CARD_HASH                  VARCHAR(16777216),
        CARD_ISSUER                VARCHAR(16777216),
        GUID                       VARCHAR(16777216),
        SESSION_COOKIE             VARCHAR(16777216),
        APPLE_MAPS_ADD             VARCHAR(16777216),
        order_identifier           VARCHAR(16777216),
        CREATED_TS                 TIMESTAMP
    )
    """
    sf.run_query(sql)


def create_temp_frame(lookback_days=5):
    print("Creating FF-excluded temp frame...")
    sf.run_query(f"DROP TABLE IF EXISTS {TEMP_FRAME}")

    diag = sf.run_query(f"""
        SELECT COUNT(*) AS total,
               COUNT(ff.output_apple_maps_normalized_address_unit_stripped) AS ff_excluded
        FROM {FRAME_TABLE} tf
        LEFT JOIN {FF_TABLE} ff
          ON ff.output_apple_maps_normalized_address_unit_stripped = tf.APPLE_MAPS_ADD
        WHERE tf.EVENT_TS >= current_date - {lookback_days}
    """)
    total    = int(diag["total"][0])
    excluded = int(diag["ff_excluded"][0])
    pct      = (excluded / total * 100) if total else 0.0
    print(f"FF Diagnostic: {total:,} total | {excluded:,} excluded ({pct:.1f}%) | {total - excluded:,} kept")

    t0 = time.time()
    sf.run_query(f"""
        CREATE TEMP TABLE {TEMP_FRAME} AS
        SELECT tf.*
        FROM {FRAME_TABLE} tf
        LEFT JOIN {FF_TABLE} ff
          ON ff.output_apple_maps_normalized_address_unit_stripped = tf.APPLE_MAPS_ADD
        WHERE tf.EVENT_TS >= current_date - {lookback_days}
          AND ff.output_apple_maps_normalized_address_unit_stripped IS NULL
    """)
    dur = time.time() - t0
    print(f"Temp frame ready in {dur:.2f}s")
    return dur


def cleanup_temp_frame():
    sf.run_query(f"DROP TABLE IF EXISTS {TEMP_FRAME}")


# -------------------------------------------------------------------------
# Per-pattern execution + enrichment
# -------------------------------------------------------------------------
ENRICH_TEMPLATE = """
SELECT
    t.order_identifier,
    '{pattern_name}'   AS hunting_pattern,
    '{query_type}'     AS query_type,
    {action_level_sql} AS ACTION_LEVEL,
    tf.ORDER_ID, tf.WEB_ORDER_ID, tf.EVENT_TS, tf.COMMIT_CD, tf.COMMIT_DT,
    tf.SALES_ORG, tf.PO_TYPE, tf.SALES_DISTRICT, tf.DISCOUNT_STORE_TYPE,
    tf.SLA, tf.EXCEPTION_CD,
    tf.B2_NM, tf.B2_EM, tf.B2_EM_DOM, tf.ACCT_AGE,
    tf.B2_ADD, tf.B2_CITY, tf.B2_DISTRICT, tf.B2_STATE, tf.B2_ZIP, tf.B2_PH,
    tf.A1_NM, tf.A1_EM, tf.A1_EM_DOM,
    tf.A1_ADD, tf.A1_CITY, tf.A1_DISTRICT, tf.A1_STATE, tf.A1_ZIP, tf.A1_PHONE,
    tf.LATEST_A1_NM, tf.LATEST_A1_EM, tf.LASTEST_A1_EM_DOM,
    tf.LATEST_A1_ADD, tf.LATEST_A1_STATE, tf.LATEST_A1_ZIP,
    tf.LATEST_BOPIS3_NM, tf.LATEST_BOPIS3_EM,
    tf.PROD_DESC, tf.TOTAL_VALUE,
    tf.IP_ADD, tf.IP_CLASS, tf.IP_COMPANY, tf.IP_COUNTRY, tf.IP_BIN_COUNTRY,
    tf.PAYMENT_TYPES, tf.CARD_BIN, tf.BIN_COUNTRY, tf.CARD_HASH, tf.CARD_ISSUER,
    tf.GUID, tf.SESSION_COOKIE, tf.APPLE_MAPS_ADD,
    CURRENT_TIMESTAMP AS CREATED_TS
FROM (
    SELECT
        COALESCE(
            TRY_CAST(web_order_id AS VARCHAR),
            TRY_CAST(order_id     AS VARCHAR),
            'UNKNOWN'
        ) AS order_identifier
        {select_action_level}
    FROM ({inner_query})
    WHERE COALESCE(
        TRY_CAST(web_order_id AS VARCHAR),
        TRY_CAST(order_id     AS VARCHAR)
    ) IS NOT NULL
) t
LEFT JOIN {temp_frame} tf
    ON t.order_identifier = COALESCE(tf.WEB_ORDER_ID, tf.ORDER_ID::VARCHAR)
"""


def execute_pattern(pattern_name, query_type, inner_query):
    if query_type == "GCT":
        action_level_sql    = "COALESCE(t.action_level, 'REVIEW')"
        select_action_level = ", action_level"
    elif "AMR" in pattern_name.upper():
        action_level_sql    = "'HIGH_RISK'"
        select_action_level = ""
    else:
        action_level_sql    = "'REVIEW'"
        select_action_level = ""

    enriched = ENRICH_TEMPLATE.format(
        pattern_name=pattern_name,
        query_type=query_type,
        action_level_sql=action_level_sql,
        select_action_level=select_action_level,
        inner_query=inner_query,
        temp_frame=TEMP_FRAME,
    )

    print(f"  {pattern_name}: ", end="")
    t0 = time.time()
    try:
        df  = sf.run_query(enriched)
        dur = time.time() - t0
        n   = 0 if df is None or df.empty else len(df)
        print(f"{n} orders in {dur:.3f}s")
        return df, n
    except Exception as e:
        print(f"ERROR -- {e}")
        return pd.DataFrame(), 0


# -------------------------------------------------------------------------
# Write + log
# -------------------------------------------------------------------------
def write_results(combined):
    if combined.empty:
        print("No data to write")
        return False

    print(f"Writing {len(combined):,} rows to {RESULTS_TABLE}")
    sf.run_query(f"TRUNCATE TABLE {RESULTS_TABLE}")
    combined = combined.reset_index(drop=True)

    conn = sf._backend._engine.connect()
    try:
        write_pandas(
            conn=conn.connection,
            df=combined,
            database="gbi_fraud_bap_db",
            schema="ai_live_biz_app",
            table_name="Trader_Automated_Queries_Results",
            quote_identifiers=False,
            auto_create_table=False,
            use_logical_type=True,
        )
        conn.connection.commit()
        print(f"Wrote {len(combined):,} rows")
        return True
    except Exception as e:
        print(f"Write failed: {e}")
        return False
    finally:
        conn.close()


def append_to_logging():
    print("Appending to logging table...")
    t0 = time.time()
    sf.run_query(f"""
        INSERT INTO {LOGGING_TABLE}
        SELECT CURRENT_TIMESTAMP(),
               ORDER_ID, WEB_ORDER_ID, HUNTING_PATTERN, QUERY_TYPE
        FROM {RESULTS_TABLE}
    """)
    cnt = sf.run_query(f"""
        SELECT COUNT(*) AS cnt FROM {LOGGING_TABLE}
        WHERE RUN_TS >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
    """)
    n = int(cnt["cnt"][0]) if not cnt.empty else 0
    print(f"Logged {n} rows in {time.time()-t0:.2f}s")
    return n


# -------------------------------------------------------------------------
# Slack formatting
# -------------------------------------------------------------------------
def format_slack(pattern_metrics, total, gct, th, runtime):
    msg  = "Trader Alerts Results\n"
    msg += "----------------------\n\n"

    gct_p = {p: v for p, v in pattern_metrics.items() if v["type"] == "GCT"}
    th_p  = {p: v for p, v in pattern_metrics.items() if v["type"] == "TH"}

    if any(v["count"] > 0 for v in gct_p.values()):
        msg += "Genuine Customer Queries\n"
        for p, v in gct_p.items():
            if v["count"] > 0:
                msg += f"{p}: {v['count']} orders\n"
        msg += "\n"

    if any(v["count"] > 0 for v in th_p.values()):
        msg += "Trader Hunting Queries\n"
        for p, v in th_p.items():
            if v["count"] > 0:
                msg += f"{p}: {v['count']} orders\n"
        msg += "\n"

    msg += "----------------------\n"
    msg += f"Total: {total} orders\n"
    msg += f"Runtime: {runtime:.1f}s\n\n"
    msg += f"GCT: {gct} | TH: {th}\n\n"
    msg += f"SELECT * FROM {RESULTS_TABLE}"
    return msg


def write_slack_file(msg, path=DEFAULT_SLACK_FILE):
    with open(path, "w") as f:
        f.write(msg)
    print(f"Slack message -> {path}")


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
def main(args):
    set_role(args.table_schema)

    query_files_dir = os.path.abspath(args.query_files_dir)
    slack_file_path = os.path.abspath(args.slack_file)

    ex_items = parse_exclusion_list(args.exclusion_list)
    ignore   = {os.path.join(query_files_dir, q) for q in ex_items}
    print(f"\nExclusion List: {sorted(ignore)}\n")

    sql_files = sorted(glob.glob(os.path.join(query_files_dir, "*.sql")))
    print(f"Found {len(sql_files)} .sql files in {query_files_dir}")

    create_results_table()
    temp_dur = create_temp_frame(lookback_days=args.lookback_days)

    total_start        = time.time()
    pattern_metrics    = {}   # name -> {"count": int, "type": str}
    all_results        = []
    pattern_total_dur  = 0.0

    try:
        for path in sql_files:
            if path in ignore:
                print(f"{path} excluded")
                continue

            pattern = pattern_name_from_path(path)
            with open(path) as f:
                sql_text = f.read()

            qtype = read_query_type_override(sql_text) or infer_query_type(pattern)
            inner = extract_select_from_ddl(sql_text)
            if not inner:
                print(f"{pattern}: could not extract SELECT -- skipping")
                pattern_metrics[pattern] = {"count": 0, "type": qtype}
                continue

            print(f"\n>> {pattern}  ({qtype})")
            t0          = time.time()
            df, n       = execute_pattern(pattern, qtype, inner)
            pattern_total_dur += time.time() - t0
            pattern_metrics[pattern] = {"count": n, "type": qtype}
            if not df.empty:
                all_results.append(df)

        runtime = time.time() - total_start

        if all_results:
            combined = pd.concat(all_results, ignore_index=True)
            pre = len(combined)
            combined = combined.drop_duplicates(subset=["order_identifier"], keep="first")
            print(f"\nDedup: {pre:,} -> {len(combined):,} unique orders")

            t_w  = time.time()
            ok   = write_results(combined)
            w_d  = time.time() - t_w

            if ok:
                if os.environ.get("UPDATE_HIST_TABLE", "").lower() in ("1", "true", "yes"):
                    append_to_logging()

                gct_n = int((combined["query_type"] == "GCT").sum())
                th_n  = int((combined["query_type"] == "TH").sum())
                msg   = format_slack(pattern_metrics, len(combined), gct_n, th_n, runtime)
                print(f"\n{msg}")
                write_slack_file(msg, path=slack_file_path)
                print(
                    f"\nBreakdown: Temp({temp_dur:.1f}s) + "
                    f"Patterns({pattern_total_dur:.1f}s) + "
                    f"Write({w_d:.1f}s) = Total({runtime:.1f}s)"
                )
            else:
                write_slack_file(
                    "Trader Hunting Pipeline: write to results table FAILED",
                    path=slack_file_path,
                )
                sys.exit(1)
        else:
            msg = (
                "Trader Alerts Results\n"
                "----------------------\n\n"
                "No suspicious orders detected\n\n"
                "----------------------\n"
                f"Total: 0 orders\n"
                f"Runtime: {runtime:.1f}s"
            )
            print(f"\n{msg}")
            write_slack_file(msg, path=slack_file_path)

    except Exception as e:
        runtime = time.time() - total_start
        err = (
            "Trader Hunting Pipeline Failed\n"
            "----------------------\n\n"
            f"Error: {e}\n"
            f"Runtime: {runtime:.1f}s"
        )
        print(f"\n{err}")
        write_slack_file(err, path=slack_file_path)
        raise
    finally:
        cleanup_temp_frame()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="TFATraderHuntingQueriesToTable")
    parser.add_argument("--table_schema",    type=str, default="GBI_FRAUD_BAP_DB.AI_LIVE_BIZ_APP")
    parser.add_argument("--table_name",      type=str, default="Trader_Automated_Queries_Results")
    parser.add_argument(
        "--query_files_dir",
        type=str,
        default=DEFAULT_QUERY_FILES_DIR,
        help="Directory of *.sql query files. Defaults to ./active_queries next to this script.",
    )
    parser.add_argument("--exclusion_list",  dest="exclusion_list", type=str)
    parser.add_argument("--lookback_days",   type=int, default=5)
    parser.add_argument(
        "--slack_file",
        type=str,
        default=DEFAULT_SLACK_FILE,
        help="Where to write the Slack summary. Defaults next to this script.",
    )
    main(parser.parse_args())