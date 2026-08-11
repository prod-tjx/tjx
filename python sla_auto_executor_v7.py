#!/usr/bin/env python3
"""
sla_auto_executor_v7.py

Automated SLA order execution script.
Queries Snowflake, evaluates V53/V54/V57 + item count flags,
and actions orders via ACM API.

Threshold change: flags any V53/V54/V57 value >= 7 (was > 10).

Usage:
    python sla_auto_executor_v7.py --once                # run once
    python sla_auto_executor_v7.py --now --minutes 0,30  # run now, then on schedule
    python sla_auto_executor_v7.py --minutes 27,57        # schedule only
    python sla_auto_executor_v7.py --now --minutes 27,57
    python sla_auto_executor_v7.py                       # default :25 and :55
"""

import time
import json
import logging
import argparse
from datetime import datetime

import pandas as pd
from ai.snowflake.compat.connection import sf

from acm_trader_helper import ACMTraderHelper

# ──────────────────────────────────────────────
#  Configuration
# ──────────────────────────────────────────────

SLA_QUERY = """
SELECT *
FROM   gbi_fraud_bap_db.ai_live_biz_app.trader_tfa_aso_ssla           jpp3
LEFT JOIN gbi_fraud_bap_db.ai_live_biz_app.TRADER_TFA_A1ADD_VELOCITY_CLUSTER_RELAXED_TEST velo_test
    ON jpp3.web_order_id = velo_test.web_order_id
"""

VELOCITY_COLUMNS = ["V53", "V54", "V57"]

ITEM_COUNT_COLUMNS = [
    "SESSION_COOKIE_ITEM_COUNT",
    "B2_EM_ITEM_COUNT",
    "A1_EM_ITEM_COUNT",
    "CARD_HASH_ITEM_COUNT",
    "B2_PH_ITEM_COUNT",
    "IP_ADD_ITEM_COUNT",
]

DUE_IN_HOUR_THRESHOLD = 24
VELOCITY_THRESHOLD = 7          # ← flag any V53/V54/V57 value >= this

CASE_NOTES = "SSLA"
ACM_ENV = "prod"
TRIGGER_MINUTES = {25, 55}

# ──────────────────────────────────────────────
#  Logging
# ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sla_auto")

# ──────────────────────────────────────────────
#  Decision Logic
# ──────────────────────────────────────────────

def check_velocity_exceeds(row) -> list:
    """Check V53/V54/V57 numeric columns. Flag if any >= VELOCITY_THRESHOLD (7)."""
    reasons = []
    for col in VELOCITY_COLUMNS:
        val = row.get(col)
        if val is None:
            continue
        try:
            num = float(val)
            if num >= VELOCITY_THRESHOLD:
                reasons.append(f"{col}={int(num)} (>= {VELOCITY_THRESHOLD})")
        except (ValueError, TypeError):
            pass
    return reasons


def check_item_count_flags(row) -> list:
    """
    Parse each *_ITEM_COUNT JSON column.
    Look for keys starting with V53/V54/V57.
    If value contains "Y" → flag it.
    """
    reasons = []
    for col in ITEM_COUNT_COLUMNS:
        raw = row.get(col)
        if not raw or str(raw).strip() == "":
            continue
        try:
            parsed = json.loads(str(raw))
        except (json.JSONDecodeError, TypeError):
            continue
        for key, val in parsed.items():
            if any(key.upper().startswith(v) for v in VELOCITY_COLUMNS):
                if "Y" in str(val).upper():
                    reasons.append(f'{col} → {key}="{val}" (Y flag)')
    return reasons


def evaluate_row(row) -> str:
    """
    TRADERCONFIRM if:
      - ANY V53/V54/V57 numeric value >= 7
      - OR any ITEM_COUNT column has a V53/V54/V57 entry with "Y"
    Otherwise → TRADERCLEAR
    """
    vel_reasons = check_velocity_exceeds(row)
    flag_reasons = check_item_count_flags(row)
    if vel_reasons or flag_reasons:
        return "TRADERCONFIRM"
    return "TRADERCLEAR"

# ──────────────────────────────────────────────
#  Data Pipeline
# ──────────────────────────────────────────────

def fetch_sla_data() -> pd.DataFrame:
    """Query Snowflake and return the SLA result set."""
    log.info("Querying Snowflake …")
    df = sf.run_query(SLA_QUERY)
    df.columns = [c.upper() for c in df.columns]
    log.info(f"Fetched {len(df)} rows from Snowflake.")
    return df


def filter_due_soon(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only orders where DUE_IN_HOUR < threshold (24)."""
    col = "DUE_IN_HOUR"
    if col not in df.columns:
        raise KeyError(f"Column '{col}' not found in query results.")

    df[col] = pd.to_numeric(df[col], errors="coerce")
    before = len(df)
    df = df[df[col] < DUE_IN_HOUR_THRESHOLD].copy()
    log.info(
        f"Filtered DUE_IN_HOUR < {DUE_IN_HOUR_THRESHOLD}: "
        f"{before} → {len(df)} orders"
    )
    return df


def build_action_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Evaluate each row → produce dataframe for ACMTraderHelper:
        order_id | action | notes
    """
    oid_col = None
    for c in df.columns:
        if c.upper() == "WEB_ORDER_ID":
            oid_col = c
            break
    if oid_col is None:
        raise KeyError("Could not find WEB_ORDER_ID column in query results.")

    records = []
    for _, row in df.iterrows():
        action = evaluate_row(row)
        records.append({
            "order_id": row[oid_col],
            "action":   action,
            "notes":    CASE_NOTES,
        })

    # reset_index ensures ACMTraderHelper can always find row [0]
    return pd.DataFrame(records).reset_index(drop=True)


def execute_actions(action_df: pd.DataFrame) -> None:
    """
    Send actions to ACM API.
    Confirms and releases are sent as SEPARATE batches
    (ACM cannot process both action types in the same call).
    """
    if action_df.empty:
        log.info("Nothing to action — dataframe is empty.")
        return

    # reset_index on slices so ACMTraderHelper always sees index starting at 0
    confirm_df = action_df[action_df["action"] == "TRADERCONFIRM"].copy().reset_index(drop=True)
    release_df = action_df[action_df["action"] == "TRADERCLEAR"].copy().reset_index(drop=True)

    log.info(
        f"Actioning {len(action_df)} orders  "
        f"(CONFIRM: {len(confirm_df)} | RELEASE: {len(release_df)})"
    )

    # ── Batch 1: Confirms ──
    if not confirm_df.empty:
        log.info(f"Sending TRADERCONFIRM batch ({len(confirm_df)} orders) …")
        acm_confirm = ACMTraderHelper(env=ACM_ENV)
        acm_confirm.df_for_action(confirm_df)
        acm_confirm.action_cases()
        log.info(f"  CONFIRM mass-action key: {acm_confirm.mass_action_key}")

    # ── Batch 2: Releases ──
    if not release_df.empty:
        log.info(f"Sending TRADERCLEAR batch ({len(release_df)} orders) …")
        acm_release = ACMTraderHelper(env=ACM_ENV)
        acm_release.df_for_action(release_df)
        acm_release.action_cases()
        log.info(f"  RELEASE mass-action key: {acm_release.mass_action_key}")

# ──────────────────────────────────────────────
#  Single Run
# ──────────────────────────────────────────────

def run_once() -> None:
    """Full pipeline: fetch → filter → evaluate → action."""
    start = datetime.now()
    log.info("═══ SLA run started ═══")

    try:
        raw_df = fetch_sla_data()
        if raw_df.empty:
            log.warning("Query returned 0 rows — skipping.")
            return

        raw_df = filter_due_soon(raw_df)
        if raw_df.empty:
            log.info("No orders with DUE_IN_HOUR < 24 — skipping.")
            return

        action_df = build_action_df(raw_df)

        summary = action_df["action"].value_counts().to_dict()
        log.info(f"Action breakdown: {summary}")

        execute_actions(action_df)

    except Exception:
        log.exception("Run failed with an error:")

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"═══ SLA run finished in {elapsed:.1f}s ═══")

# ──────────────────────────────────────────────
#  Scheduler Loop
# ──────────────────────────────────────────────

def scheduler_loop() -> None:
    """Fires at configured TRIGGER_MINUTES past each hour."""
    log.info(f"Scheduler started — triggers at minutes {sorted(TRIGGER_MINUTES)}")
    already_ran = None

    while True:
        now = datetime.now()
        if now.minute in TRIGGER_MINUTES and already_ran != now.minute:
            already_ran = now.minute
            run_once()
        elif now.minute not in TRIGGER_MINUTES:
            already_ran = None
        time.sleep(15)

# ──────────────────────────────────────────────
#  Entry Point
# ──────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SLA Auto-Execution Script")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--now", action="store_true", help="Run immediately, then continue on schedule.")
    parser.add_argument("--minutes", type=str, help="Comma-separated minutes to trigger (e.g. 0,30)")
    args = parser.parse_args()

    if args.minutes:
        TRIGGER_MINUTES.clear()
        TRIGGER_MINUTES.update(int(m) for m in args.minutes.split(","))

    if args.once:
        run_once()
    else:
        if args.now:
            run_once()
        scheduler_loop()