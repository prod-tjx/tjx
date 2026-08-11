from __future__ import annotations
# trader_test_gibberish_ff_filtered.py
# TEST — AMR_gibberish_em_a1add_velocity
# FF orders excluded at temp table creation via tfa_aso_freight_forwarder_address_log
# Join key: APPLE_MAPS_ADD (normalized) not A1_ADD (raw)

from ai.snowflake.compat.connection import sf
from typing import Optional
import pandas as pd
import re
import time

PATTERN_NAME = 'AMR_gibberish_em_a1add_velocity'
QUERY_TYPE   = 'TH'


# ─────────────────────────────────────────────────────────────────────────────
# Queries 1–3: Temp table + FF diagnostic
# ─────────────────────────────────────────────────────────────────────────────

def create_temp_table() -> float:
    """
    Builds trader_temp_frame from last 5 days.
    FF join key: APPLE_MAPS_ADD (normalized Apple Maps address on the frame)
                 matched against output_apple_maps_normalized_address_unit_stripped
                 in tfa_aso_freight_forwarder_address_log — same pattern as MR script.
    Diagnostic query runs first so you can see exactly how many orders were cut.
    """
    # Query 1 — drop
    sf.run_query("DROP TABLE IF EXISTS gbi_fraud_bap_db.ai_live_biz_app.trader_temp_frame")

    # Query 2 — FF diagnostic: how many orders match the FF log before we filter
    print("🔍 Running FF diagnostic...")
    diagnostic = sf.run_query("""
        SELECT
            COUNT(*)                                                         AS total_rows,
            COUNT(ff.output_apple_maps_normalized_address_unit_stripped)     AS ff_excluded,
            COUNT(*) - COUNT(ff.output_apple_maps_normalized_address_unit_stripped) AS rows_kept
        FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame tf

        LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.tfa_aso_freight_forwarder_address_log ff
            ON ff.output_apple_maps_normalized_address_unit_stripped = tf.APPLE_MAPS_ADD

        WHERE tf.EVENT_TS >= CURRENT_DATE - 5
    """)

    total    = int(diagnostic['total_rows'][0])
    excluded = int(diagnostic['ff_excluded'][0])
    kept     = int(diagnostic['rows_kept'][0])
    pct      = (excluded / total * 100) if total > 0 else 0

    print(f"📊 FF Diagnostic (last 5d):")
    print(f"   Total rows           : {total:,}")
    print(f"   FF excluded          : {excluded:,}  ({pct:.1f}%)")
    print(f"   Rows entering temp   : {kept:,}")

    # Query 3 — CREATE temp with FF filter applied
    # APPLE_MAPS_ADD is the normalized field that aligns with the FF address log
    sql = """
        CREATE TEMP TABLE trader_temp_frame AS
        SELECT tf.*
        FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame tf

        LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.tfa_aso_freight_forwarder_address_log ff
            ON ff.output_apple_maps_normalized_address_unit_stripped = tf.APPLE_MAPS_ADD

        WHERE tf.EVENT_TS >= CURRENT_DATE - 5
          AND ff.output_apple_maps_normalized_address_unit_stripped IS NULL
    """

    start = time.time()
    sf.run_query(sql)
    duration = time.time() - start
    print(f"✅ Temp table created (FF orders excluded) in {duration:.2f}s")
    return duration


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_trader_hunting_query(hunting_pattern: str) -> str:
    # Query 4
    df = sf.run_query(f"""
        SELECT get_ddl('view', 'gbi_fraud_bap_db.ai_live_biz_app.{hunting_pattern}') AS query
    """)
    return df['query'][0]


def transform_query(query: str) -> Optional[str]:
    try:
        as_match = re.search(r'\bAS\s+(.*)', query, re.IGNORECASE | re.DOTALL)
        if not as_match:
            print("❌ Could not find AS clause in query")
            return None

        extracted = as_match.group(1).strip()
        if extracted.startswith('(') and extracted.endswith(')'):
            extracted = extracted[1:-1].strip()

        extracted = re.sub(r';+\s*$', '', extracted)

        return re.sub(
            r"gbi_trader_semantic_db\.ai\.trader_orders",
            r"gbi_fraud_bap_db.ai_live_biz_app.trader_orders",
            extracted,
            flags=re.IGNORECASE
        )
    except Exception as e:
        print(f"❌ Transform error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Query 5: Pattern execution — FF already excluded via temp table
# ─────────────────────────────────────────────────────────────────────────────

def execute_pattern() -> pd.DataFrame:
    print(f"🚀 Running: {PATTERN_NAME}")

    raw_query = get_trader_hunting_query(PATTERN_NAME)
    pattern_query = transform_query(raw_query)

    if not pattern_query:
        print("❌ Could not extract pattern query")
        return pd.DataFrame()

    pattern_query = pattern_query.replace(
        'gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame',
        'trader_temp_frame'
    )

    # Query 5
    enriched_query = f"""
        SELECT
            t.order_identifier,
            '{PATTERN_NAME}'    AS hunting_pattern,
            '{QUERY_TYPE}'      AS query_type,
            'HIGH_RISK'         AS ACTION_LEVEL,
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
            tf.APPLE_MAPS_ADD,
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
            CURRENT_TIMESTAMP   AS CREATED_TS

        FROM (
            SELECT COALESCE(
                       TRY_CAST(web_order_id AS VARCHAR),
                       TRY_CAST(order_id    AS VARCHAR),
                       'UNKNOWN'
                   ) AS order_identifier
            FROM ({pattern_query})
            WHERE COALESCE(
                      TRY_CAST(web_order_id AS VARCHAR),
                      TRY_CAST(order_id    AS VARCHAR)
                  ) IS NOT NULL
        ) t

        LEFT JOIN trader_temp_frame tf
            ON t.order_identifier = COALESCE(tf.WEB_ORDER_ID, tf.ORDER_ID::VARCHAR)
    """

    start = time.time()
    result = sf.run_query(enriched_query)
    duration = time.time() - start

    row_count = len(result) if not result.empty else 0
    print(f"✅ {row_count} orders (FF excluded) returned in {duration:.3f}s")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print(f"🧪 TEST — {PATTERN_NAME}")
    print("=" * 60)
    total_start = time.time()

    try:
        create_temp_table()
        result = execute_pattern()

        if not result.empty:
            # Preview using confirmed columns from the schema
            preview_cols = [
                'WEB_ORDER_ID', 'APPLE_MAPS_ADD', 'A1_ADD',
                'A1_STATE', 'A1_ZIP', 'TOTAL_VALUE'
            ]
            available = [c for c in preview_cols if c in result.columns]
            print(f"\nSample ({min(10, len(result))} of {len(result)} rows):")
            print(result[available].head(10).to_string(index=False))
        else:
            print("ℹ️  No results after FF exclusion")

    finally:
        sf.run_query("DROP TABLE IF EXISTS trader_temp_frame")
        print(f"\n⏱️  Total runtime: {time.time() - total_start:.2f}s")


if __name__ == '__main__':
    main()