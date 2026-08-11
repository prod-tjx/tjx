# trader_graph_data_pilot_uat.py
# Automated scoring monitor — runs on 4-hour schedule aligned with
# trader_tfa_ww_mass_review Jenkins refresh cadence.
#
# JENKINS SETUP
# -------------
# Schedule this job with the same trigger as the mass review refresh.
# Cron expression for every 4 hours: H */4 * * *
# (Use H rather than 0 to distribute load — ask your Jenkins admin to
# match the offset used by the mass review refresh job.)
#
# This script runs silently in the background to capture a baseline of
# TFA actions BEFORE the scoring layer is introduced to the team during UAT.
# No output is visible to TFAs — it only writes to trader_scoring_monitor.
#
# Once the scoring layer is introduced, this job continues running on the
# same schedule. The score_date column in the snapshot table marks the
# before/after split point automatically.
#
# TABLE DESIGN
# ------------
# One row per order, written at induction and never duplicated.
# recommended_action / trader_risk_score / primary_signal hold the induction-time
# score and are immutable after INSERT.
# recommended_action_current / trader_risk_score_current / primary_signal_current
# are refreshed on every run while the order is unresolved, capturing how the
# recommendation evolves as the order sits in the backlog.
# This enables dashboard questions like: "did the recommendation upgrade before
# the TFA acted, and did they follow it?"
#
# SCHEMA MIGRATION (run once before deploying this version)
# ---------------------------------------------------------
# ALTER TABLE gbi_fraud_bap_db.ai_live_biz_app.trader_scoring_monitor
#     ADD COLUMN IF NOT EXISTS recommended_action_current  VARCHAR
#    ,ADD COLUMN IF NOT EXISTS trader_risk_score_current   INTEGER
#    ,ADD COLUMN IF NOT EXISTS primary_signal_current      VARCHAR;
#
# MARKETS IN SCOPE
# ----------------
# India (1320) and Japan (8400). To add a market, update the UAT_MARKETS
# tuple below.

from ai.snowflake.compat.connection import sf
import time

UAT_MARKETS = ('1320', '8400')


# ── SQL definitions ────────────────────────────────────────────────────────────

INGEST_SQL = f"""
INSERT INTO gbi_fraud_bap_db.ai_live_biz_app.trader_scoring_monitor
    (order_id, web_order_id, sales_org, score_date,
     trader_risk_score, recommended_action, primary_signal,
     component_ct_rate, auto_label, induction_date,
     recommended_action_current, trader_risk_score_current, primary_signal_current)

WITH olss_flagged AS (
    SELECT DISTINCT DATA_PERSONID, TRUE AS olss_change
    FROM GBI_FRAUD_SEMANTIC_DB.AI.KAFKA_ATHENA_NVP_AOS_WEB_ORDER
    WHERE DECISIONS_DELIVERYBLOCKRESPONSE       = 'ST'
      AND output_trader_flexible_threshold_name LIKE '%-olss-change'
      AND OUTPUT_PROBABLETRADERATTRIBUTERULE     = 'true'
      AND EVENT_DT                             >= CURRENT_DATE - 30
)
SELECT DISTINCT
     mr.ORDER_ID
    ,mr.WEB_ORDER_ID
    ,mr.SALES_ORG
    ,CURRENT_DATE                                                AS score_date
    ,CASE
         WHEN mr.OOCA:out_of_country_activation_rate::FLOAT > 0
          AND COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30         THEN 99
         WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.70         THEN 90
         WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.50         THEN 80
         WHEN olss.olss_change = TRUE
          AND COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30         THEN 75
         WHEN olss.olss_change = TRUE                           THEN 70
         WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30         THEN 65
         ELSE 0
     END                                                         AS trader_risk_score
    ,CASE
         WHEN mr.OOCA:out_of_country_activation_rate::FLOAT > 0
          AND COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30         THEN 'AUTO_CONFIRM'
         WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.70         THEN 'AUTO_CONFIRM'
         WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.50         THEN 'FAST_TRACK_CONFIRM'
         WHEN olss.olss_change = TRUE                           THEN 'FAST_TRACK_CONFIRM'
         WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30         THEN 'FAST_TRACK_REVIEW'
         ELSE 'STANDARD_REVIEW'
     END                                                         AS recommended_action  
    ,CASE
         WHEN mr.OOCA:out_of_country_activation_rate::FLOAT > 0
          AND COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30
              THEN 'ooc+component_ct_rate_' || ROUND(gl.COMPONENT_CT_RATE, 2)::VARCHAR
         WHEN olss.olss_change = TRUE
          AND COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30
              THEN 'olss_change+component_ct_rate_' || ROUND(gl.COMPONENT_CT_RATE, 2)::VARCHAR
         WHEN olss.olss_change = TRUE
              THEN 'olss_change'
         WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) > 0
              THEN 'component_ct_rate'
         ELSE NULL
     END                                                         AS primary_signal
         
    ,COALESCE(gl.COMPONENT_CT_RATE, 0)                          AS component_ct_rate
    ,NULL                                                        AS auto_label
    ,CURRENT_DATE                                                AS induction_date
    -- current columns initialised to induction values; refreshed by RESCORE_SQL on each run
    ,CASE
         WHEN mr.OOCA:out_of_country_activation_rate::FLOAT > 0
          AND COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30         THEN 'AUTO_CONFIRM'
         WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.70         THEN 'AUTO_CONFIRM'
         WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.50         THEN 'FAST_TRACK_CONFIRM'
         WHEN olss.olss_change = TRUE                           THEN 'FAST_TRACK_CONFIRM'
         WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30         THEN 'FAST_TRACK_REVIEW'
         ELSE 'STANDARD_REVIEW'
     END                                                         AS recommended_action_current
    ,CASE
         WHEN mr.OOCA:out_of_country_activation_rate::FLOAT > 0
          AND COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30         THEN 99
         WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.70         THEN 90
         WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.50         THEN 80
         WHEN olss.olss_change = TRUE
          AND COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30         THEN 75
         WHEN olss.olss_change = TRUE                           THEN 70
         WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30         THEN 65
         ELSE 0
     END                                                         AS trader_risk_score_current
    ,CASE
         WHEN mr.OOCA:out_of_country_activation_rate::FLOAT > 0
          AND COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30
              THEN 'ooc+component_ct_rate_' || ROUND(gl.COMPONENT_CT_RATE, 2)::VARCHAR
         WHEN olss.olss_change = TRUE
          AND COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30
              THEN 'olss_change+component_ct_rate_' || ROUND(gl.COMPONENT_CT_RATE, 2)::VARCHAR
         WHEN olss.olss_change = TRUE
              THEN 'olss_change'
         WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) > 0
              THEN 'component_ct_rate'
         ELSE NULL
     END                                                         AS primary_signal_current
FROM gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_ww_mass_review mr
LEFT JOIN GBI_FRAUD_BAP_DB.AI_LIVE_BIZ_APP.TRADER_GRAPH_ORDER_LOOKUP gl
    ON mr.WEB_ORDER_ID = gl.WEB_ORDER_ID
   AND gl.SALESORG     = mr.SALES_ORG
LEFT JOIN olss_flagged olss
    ON mr.PERSON_ID    = olss.DATA_PERSONID
WHERE mr.SALES_ORG IN {UAT_MARKETS}
  AND NOT EXISTS (
      SELECT 1
      FROM gbi_fraud_bap_db.ai_live_biz_app.trader_scoring_monitor m
      WHERE m.order_id = mr.ORDER_ID
  )
"""

RESCORE_SQL = f"""
UPDATE gbi_fraud_bap_db.ai_live_biz_app.trader_scoring_monitor m
SET    recommended_action_current  = CASE
           WHEN mr.OOCA:out_of_country_activation_rate::FLOAT > 0
            AND COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30       THEN 'AUTO_CONFIRM'
           WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.70       THEN 'AUTO_CONFIRM'
           WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.50       THEN 'FAST_TRACK_CONFIRM'
           WHEN olss.olss_change = TRUE                         THEN 'FAST_TRACK_CONFIRM'
           WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30       THEN 'FAST_TRACK_REVIEW'
           ELSE 'STANDARD_REVIEW'
       END
      ,trader_risk_score_current   = CASE
           WHEN mr.OOCA:out_of_country_activation_rate::FLOAT > 0
            AND COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30       THEN 99
           WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.70       THEN 90
           WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.50       THEN 80
           WHEN olss.olss_change = TRUE
            AND COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30       THEN 75
           WHEN olss.olss_change = TRUE                         THEN 70
           WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30       THEN 65
           ELSE 0
       END
      ,primary_signal_current      = CASE
           WHEN mr.OOCA:out_of_country_activation_rate::FLOAT > 0
            AND COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30
                THEN 'ooc+component_ct_rate_' || ROUND(gl.COMPONENT_CT_RATE, 2)::VARCHAR
           WHEN olss.olss_change = TRUE
            AND COALESCE(gl.COMPONENT_CT_RATE, 0) >= 0.30
                THEN 'olss_change+component_ct_rate_' || ROUND(gl.COMPONENT_CT_RATE, 2)::VARCHAR
           WHEN olss.olss_change = TRUE
                THEN 'olss_change'
           WHEN COALESCE(gl.COMPONENT_CT_RATE, 0) > 0
                THEN 'component_ct_rate'
           ELSE NULL
       END
FROM gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_ww_mass_review mr
LEFT JOIN GBI_FRAUD_BAP_DB.AI_LIVE_BIZ_APP.TRADER_GRAPH_ORDER_LOOKUP gl
    ON mr.WEB_ORDER_ID = gl.WEB_ORDER_ID
   AND gl.SALESORG     = mr.SALES_ORG
LEFT JOIN (
    SELECT DISTINCT DATA_PERSONID, TRUE AS olss_change
    FROM GBI_FRAUD_SEMANTIC_DB.AI.KAFKA_ATHENA_NVP_AOS_WEB_ORDER
    WHERE DECISIONS_DELIVERYBLOCKRESPONSE       = 'ST'
      AND output_trader_flexible_threshold_name LIKE '%-olss-change'
      AND OUTPUT_PROBABLETRADERATTRIBUTERULE     = 'true'
      AND EVENT_DT                             >= CURRENT_DATE - 30
) olss ON mr.PERSON_ID = olss.DATA_PERSONID
WHERE m.order_id   = mr.ORDER_ID
  AND m.final_date IS NULL
  AND mr.SALES_ORG IN {UAT_MARKETS}
"""

OUTCOME_UPDATE_SQL = """
UPDATE gbi_fraud_bap_db.ai_live_biz_app.trader_scoring_monitor m
SET    final_trader_cd      = COALESCE(os.TRADER_CD, 'PT_CLEARED')
      ,final_date           = CURRENT_DATE
      ,reviewer_override_flag = CASE
           -- Type I: model said confirm at time of TFA action, but order was cleared
           WHEN m.recommended_action_current IN ('AUTO_CONFIRM', 'FAST_TRACK_CONFIRM')
            AND (os.TRADER_CD != 'CT' OR os.TRADER_CD IS NULL)  THEN TRUE
           -- Type II: model said release, but TFA confirmed trader
           WHEN m.recommended_action_current  = 'AUTO_RELEASE'
            AND os.TRADER_CD  = 'CT'                            THEN TRUE
           ELSE FALSE
       END
FROM gbi_fraud_semantic_db.ai.order_summary os
WHERE m.order_id       = os.ORDER_ID
  AND m.final_date     IS NULL
  AND (os.TRADER_CD    IS NULL OR os.TRADER_CD != 'PT')
"""


# ── Job functions ──────────────────────────────────────────────────────────────

def run_ingest():
    """Insert net-new orders from the mass review queue.
    One row per order ever — NOT EXISTS guards on order_id with no date condition."""
    print("Running ingest...")
    start = time.time()
    sf.run_query(INGEST_SQL)
    duration = time.time() - start
    print(f"Ingest complete in {duration:.2f}s")
    return duration


def run_rescore():
    """Refresh recommended_action_current / trader_risk_score_current / primary_signal_current
    for all unresolved orders. Captures recommendation upgrades/downgrades as orders
    sit in the backlog — compared against final_trader_cd at resolution for override analysis."""
    print("Running rescore...")
    start = time.time()
    sf.run_query(RESCORE_SQL)
    duration = time.time() - start
    print(f"Rescore complete in {duration:.2f}s")
    return duration


def run_outcome_update():
    """Close out orders resolved by TFAs since the last run.
    Writes final trader code and computes override flag against recommended_action_current."""
    print("Running outcome update...")
    start = time.time()
    sf.run_query(OUTCOME_UPDATE_SQL)
    duration = time.time() - start
    print(f"Outcome update complete in {duration:.2f}s")
    return duration


def get_snapshot_counts():
    """Return a quick row count for logging — total, inducted today, resolved today."""
    result = sf.run_query("""
        SELECT
             COUNT(*)                                            AS total_scored
            ,COUNT(CASE WHEN score_date = CURRENT_DATE THEN 1 END) AS inducted_today
            ,COUNT(CASE WHEN final_date  = CURRENT_DATE THEN 1 END) AS resolved_today
        FROM gbi_fraud_bap_db.ai_live_biz_app.trader_scoring_monitor
    """)
    return result.iloc[0]


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("TRADER SCORING MONITOR — 4HR REFRESH")
    print("=" * 50)
    total_start = time.time()

    try:
        ingest_time  = run_ingest()
        rescore_time = run_rescore()
        outcome_time = run_outcome_update()

        counts = get_snapshot_counts()
        total_time = time.time() - total_start

        print(f"\nSnapshot table status:")
        print(f"  Total scored (all time): {counts['total_scored']}")
        print(f"  Inducted today:          {counts['inducted_today']}")
        print(f"  Resolved today:          {counts['resolved_today']}")
        print(f"\nCompleted in {total_time:.1f}s "
              f"(ingest {ingest_time:.1f}s + rescore {rescore_time:.1f}s"
              f" + outcomes {outcome_time:.1f}s)")

    except Exception as e:
        total_time = time.time() - total_start
        print(f"Pipeline failed after {total_time:.1f}s: {str(e)}")
        raise


if __name__ == '__main__':
    main()