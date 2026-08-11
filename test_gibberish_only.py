# test_gibberish_only.py
# Tests ONLY AMR_gibberish_em_a1add_velocity through the exact same
# code path as the production pipeline. READ ONLY — no writes.

from ai.snowflake.compat.connection import sf
import pandas as pd
import re
import time

# ── copy-pasted verbatim from production script ────────────────────────────

def get_trader_hunting_query(hunting_pattern):
    df = sf.run_query(
        f"select get_ddl('view', 'gbi_fraud_bap_db.ai_live_biz_app.{hunting_pattern}') as query"
    )
    return df['query'][0]


def transform_query(query):
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


def create_temp_table():
    print("Creating temporary table...")
    sf.run_query("DROP TABLE IF EXISTS gbi_fraud_bap_db.ai_live_biz_app.trader_temp_frame")
    diag = sf.run_query("""
        SELECT
            COUNT(*) AS total,
            COUNT(ff.output_apple_maps_normalized_address_unit_stripped) AS ff_excluded
        FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame tf
        LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.tfa_aso_freight_forwarder_address_log ff
            ON ff.output_apple_maps_normalized_address_unit_stripped = tf.APPLE_MAPS_ADD
        WHERE tf.EVENT_TS >= current_date - 5
    """)
    total    = int(diag['total'][0])
    excluded = int(diag['ff_excluded'][0])
    pct      = (excluded / total * 100) if total > 0 else 0
    print(f"📊 FF Diagnostic: {total:,} total | {excluded:,} FF excluded ({pct:.1f}%) | {total - excluded:,} kept")
    sf.run_query("""
    CREATE TEMP TABLE trader_temp_frame AS
    SELECT tf.*
    FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame tf
    LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.tfa_aso_freight_forwarder_address_log ff
        ON ff.output_apple_maps_normalized_address_unit_stripped = tf.APPLE_MAPS_ADD
    WHERE tf.EVENT_TS >= current_date - 5
      AND ff.output_apple_maps_normalized_address_unit_stripped IS NULL
    """)
    print("✅ Temp table ready")


def execute_pattern_with_enrichment(pattern_name, query_type):
    print(f"  🚀 {pattern_name}: ", end="")
    query = get_trader_hunting_query(pattern_name)
    transformed_query = transform_query(query)
    if not transformed_query:
        print("Could not extract query")
        return pd.DataFrame(), 0
    transformed_query = transformed_query.replace(
        'gbi_fraud_bap_db.ai_live_biz_app.Trader_TFA_traderhunt_frame',
        'trader_temp_frame'
    )
    if query_type == 'GCT':
        action_level_sql  = "COALESCE(t.action_level, 'REVIEW')"
        select_action_level = ", action_level"
    else:
        action_level_sql  = "'HIGH_RISK'" if 'AMR' in pattern_name.upper() else "'REVIEW'"
        select_action_level = ""
    enriched_query = f"""
        SELECT
            t.order_identifier,
            '{pattern_name}' as hunting_pattern,
            '{query_type}' as query_type,
            {action_level_sql} as ACTION_LEVEL,
            tf.ORDER_ID, tf.WEB_ORDER_ID, tf.EVENT_TS, tf.COMMIT_CD, tf.COMMIT_DT,
            tf.SALES_ORG, tf.PO_TYPE, tf.SALES_DISTRICT, tf.DISCOUNT_STORE_TYPE,
            tf.SLA, tf.EXCEPTION_CD, tf.B2_NM, tf.B2_EM, tf.B2_EM_DOM, tf.ACCT_AGE,
            tf.B2_ADD, tf.B2_CITY, tf.B2_DISTRICT, tf.B2_STATE, tf.B2_ZIP, tf.B2_PH,
            tf.A1_NM, tf.A1_EM, tf.A1_EM_DOM, tf.A1_ADD, tf.A1_CITY, tf.A1_DISTRICT,
            tf.A1_STATE, tf.A1_ZIP, tf.A1_PHONE,
            tf.LATEST_A1_NM, tf.LATEST_A1_EM, tf.LASTEST_A1_EM_DOM,
            tf.LATEST_A1_ADD, tf.LATEST_A1_STATE, tf.LATEST_A1_ZIP,
            tf.LATEST_BOPIS3_NM, tf.LATEST_BOPIS3_EM,
            tf.PROD_DESC, tf.TOTAL_VALUE,
            tf.IP_ADD, tf.IP_CLASS, tf.IP_COMPANY, tf.IP_COUNTRY, tf.IP_BIN_COUNTRY,
            tf.PAYMENT_TYPES, tf.CARD_BIN, tf.BIN_COUNTRY, tf.CARD_HASH, tf.CARD_ISSUER,
            tf.GUID, tf.SESSION_COOKIE, tf.APPLE_MAPS_ADD,
            CURRENT_TIMESTAMP as CREATED_TS
        FROM (
            SELECT COALESCE(
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


def cleanup_temp_table():
    sf.run_query("DROP TABLE IF EXISTS trader_temp_frame")


# ── test entry point ───────────────────────────────────────────────────────

def main():
    print("🧪 GIBBERISH PATTERN — ISOLATED TEST")
    print("=" * 50)
    print("READ ONLY: no writes to results or logging tables")
    print("=" * 50)

    t0 = time.time()
    try:
        create_temp_table()

        print("\nExecuting pattern:")
        result, count = execute_pattern_with_enrichment(
            'AMR_gibberish_em_a1add_velocity', 'TH'
        )

        print("\n" + "=" * 50)
        if count > 0:
            print(f"✅ SUCCESS — {count} orders returned")
            print(f"\nSample (first 5 rows):")
            print(result[['order_identifier', 'hunting_pattern',
                          'ACTION_LEVEL', 'SALES_ORG',
                          'B2_EM_DOM', 'EVENT_TS']].head(5).to_string())
        else:
            print("⚠️  0 orders returned — no error but no matches")
        print(f"\n⏱️  Total: {time.time() - t0:.2f}s")

    except Exception as e:
        print(f"\n❌ FAILED: {e}")

    finally:
        cleanup_temp_table()
        print("🧹 Temp table cleaned up")


if __name__ == '__main__':
    main()
