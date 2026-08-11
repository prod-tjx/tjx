#!/usr/bin/env python3
"""
trader_tfa_autoct_executor.py

Pulls TFA Trader Hunting backlog (TH-only, ASO-filtered to non-trader-actioned
ST-blocked orders) and applies TRADERCONFIRM via ACM A3 with a per-pattern
case note. Posts a minimal green-bar success summary to Slack on completion.

Auth pattern mirrors ai.fulfillment.trader.marketplace_cancel — uses
APP_PASSWORD (Jenkins secret-text binding to SP_TFA_A3_AUTH_IDMS_PROD)
plus --dsid passed via the execution shell. Does NOT use ACMTraderHelper
(that one uses interactive openId auth and won't work in Jenkins).

Slack: SLACK_WEBHOOK_URL env var (set in Jenkins shell). Posts only on
successful actioning of >=1 order; silent on failures or empty backlog.

DRI: tjx@apple.com
Jenkins Job: trader-tfa-AutoCT-executor
Repo: github.pie.apple.com/ai/TFA-Trader-Hunting-Automation
"""
import argparse
import json
import logging
import math
import os
from datetime import datetime

import pandas as pd
import requests
from acm_api_a3 import ACM_API_A3

from ai.snowflake.compat.connection import sf

# ──────────────────────────────────────────────
#  Config
# ──────────────────────────────────────────────
IDMS_ENDPOINTS = {
    "prod": "https://idmsservice.corp.apple.com/apptoapp/token/generate",
    "dev":  "https://idmsservice-uat.corp.apple.com/apptoapp/token/generate",
}
ACM_ENDPOINTS = {
    "prod": "acm-nonregulated.g.apple.com",
    "dev":  "acm-nonregulated-uat2.g.apple.com",
}
APP_ID        = "180284"
ACM_OTHER_APP = 2631

CONFIRM_ACTION  = "TRADERCONFIRM"
CONFIRM_REASON  = "A101"
CASE_NOTE_BASE  = "SDS Trader Confirm SL TH"

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")

BACKLOG_QUERY = """
SELECT twmr.order_id,
       twmr.hunting_pattern
FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_Automated_Queries_Results twmr

LEFT JOIN (
    SELECT DISTINCT order_id
    FROM gbi_fraud_semantic_db.ai.order_exception_action
    WHERE exception_type_cd = 'D68'
      AND CREATE_TS >= current_date - 30
      AND exception_action_taken_cd IN
          ('A100','A101','A102','A103','110','111','112','113',
           '100','101','102','103')
) tao ON twmr.order_id = tao.order_id

JOIN (
    SELECT DISTINCT salesorder
    FROM gbi_fraud_bap_db.ai_live_biz_app.aso_transactions
    WHERE COALESCE(RESOLUTION_DESC, '') NOT LIKE 'TRADER%'
      AND DELIVERYBLOCK = 'ST'
      AND event_ts >= current_date - 15
) aso ON aso.salesorder = twmr.order_id

WHERE tao.order_id IS NULL
  AND twmr.query_type = 'TH'
"""

# ──────────────────────────────────────────────
#  Logging
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("trader_tfa_autoct_executor")

# ──────────────────────────────────────────────
#  A3 auth
# ──────────────────────────────────────────────
def generate_a3_token(env, app_password):
    idms_url = IDMS_ENDPOINTS[env]
    data = {
        "appId":          int(APP_ID),
        "appPassword":    app_password,
        "otherApp":       ACM_OTHER_APP,
        "oneTimeToken":   False,
        "context":        "testcontext",
        "contextVersion": 1,
        "timeToLive":     600000,
    }
    log.info("Generating A3 token from %s", idms_url)
    r = requests.post(
        idms_url,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        data=json.dumps(data),
        timeout=30,
    )
    body = json.loads(r.content.decode("utf8"))
    if r.status_code == 200:
        log.info("A3 token generated successfully")
        return body["token"]
    log.error("Failed to generate A3 token: %s", body)
    raise RuntimeError("Failed to generate A3 token")

# ──────────────────────────────────────────────
#  ACM call
# ──────────────────────────────────────────────
def confirm_orders(env, dsid, token, identifiers, case_note):
    os.environ["AUTH_TOKEN"] = token
    acm_config = {
        "acm_server":   ACM_ENDPOINTS[env],
        "acm_uri":      "/api/auth/a3",
        "content_type": "application/json",
        "accept":       "application/json",
        "appId":        APP_ID,
        "prsId":        dsid,
        "method_name":  "mass_action",
        "data": {
            "lobId":        "AOS",
            "action":       CONFIRM_ACTION,
            "reason":       CONFIRM_REASON,
            "identifiers":  identifiers,
            "caseNoteText": case_note,
            "publicFlag":   False,
        },
    }
    log.info("ACM mass_action TRADERCONFIRM — %d orders — note=%r",
             len(identifiers), case_note)
    return ACM_API_A3.invoke_api(acm_config)


def parse_response(response):
    status_dict, _ = response
    body   = status_dict.get("body")
    status = status_dict.get("status", {})
    if body is None:
        log.error("ACM body is None — status: %s", status)
        return
    log.info("ACM resp — key=%s status=%s total=%s succeeded=%s failed=%s invalid=%s",
             body.get("key"), body.get("status"), body.get("totalCount"),
             body.get("succeeded"), body.get("failed"), body.get("invalidIds"))
    if not status.get("success"):
        log.error("ACM request not successful: %s", status)


def batch_confirm(env, dsid, token, identifiers, case_note, batch_size=500):
    n = len(identifiers)
    batches = math.ceil(n / batch_size)
    items = list(identifiers.items())
    for i in range(batches):
        chunk = dict(items[i*batch_size : min((i+1)*batch_size, n)])
        log.info("Batch %d/%d (%d orders)", i+1, batches, len(chunk))
        parse_response(confirm_orders(env, dsid, token, chunk, case_note))

# ──────────────────────────────────────────────
#  Slack notification — minimal, mirrors ASO health monitor format
# ──────────────────────────────────────────────
def format_slack_payload(total_orders, pattern_count):
    """Minimal green-bar success message."""
    text = (
        f":white_check_mark: AutoCT actioned *{total_orders}* orders "
        f"across *{pattern_count}* pattern{'s' if pattern_count != 1 else ''}"
    )
    return {
        "text": text,
        "username":   "TFA Trader Notification",
        "icon_emoji": ":robot_face:",
        "attachments": [{
            "color": "good",
            "fields": [
                {"title": "Orders Confirmed", "value": str(total_orders),  "short": True},
                {"title": "Patterns",         "value": str(pattern_count), "short": True},
            ],
            "footer": "TFA Trader AutoCT Executor",
        }],
    }


def post_success_to_slack(total_orders, pattern_count):
    if not SLACK_WEBHOOK:
        log.warning("No SLACK_WEBHOOK_URL set — skipping Slack post")
        return
    if total_orders == 0:
        return
    try:
        r = requests.post(
            SLACK_WEBHOOK,
            json=format_slack_payload(total_orders, pattern_count),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if r.status_code == 200:
            log.info("✅ Slack notification sent")
        else:
            log.error("❌ Slack notification failed: %s — %s", r.status_code, r.text)
    except Exception as e:
        log.error("❌ Slack post threw: %s", e)

# ──────────────────────────────────────────────
#  Pipeline
# ──────────────────────────────────────────────
def fetch_backlog() -> pd.DataFrame:
    log.info("Querying TH backlog (ASO-filtered, TH-only) …")
    df = sf.run_query(BACKLOG_QUERY)

    # Empty-result guard: when the query returns 0 rows, pandas may drop
    # column metadata, so any df["HUNTING_PATTERN"] reference would crash.
    # Return an empty DataFrame with the expected schema so main() can
    # handle it cleanly via df.empty.
    if df is None or df.empty:
        log.info("Backlog: 0 unique TH orders across 0 patterns")
        return pd.DataFrame(columns=["ORDER_ID", "HUNTING_PATTERN"])

    df.columns = [c.upper() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.drop_duplicates(subset=["ORDER_ID"], keep="first").reset_index(drop=True)
    log.info("Backlog: %d unique TH orders across %d patterns",
             len(df), df["HUNTING_PATTERN"].nunique())
    return df


def main():
    parser = argparse.ArgumentParser(description="TFA Trader AutoCT executor")
    parser.add_argument("--env",  required=True, choices=["dev", "prod"],
                        help="dev=UAT, prod=production")
    parser.add_argument("--dsid", required=True, help="Service account DSID for ACM")
    args = parser.parse_args()

    app_password = os.environ.get("APP_PASSWORD")
    if not app_password:
        raise RuntimeError("APP_PASSWORD env var required (Jenkins secret binding)")

    start = datetime.now()
    log.info("═══ Trader AutoCT — env=%s dsid=%s ═══", args.env, args.dsid)

    total_actioned = 0
    pattern_count  = 0

    try:
        df = fetch_backlog()
        if df.empty:
            log.info("No TH orders to action — exiting (no Slack post)")
            return

        token = generate_a3_token(args.env, app_password)

        for pattern, group in df.groupby("HUNTING_PATTERN"):
            case_note   = f'{CASE_NOTE_BASE} - "{pattern}"'
            # Map every order_id → the pattern's case note so ACM applies
            # the SAME note to every order in the batch (the dict VALUE is
            # the per-identifier case note in mass_action; using oid as the
            # value caused order_ids to land in the case-note field).
            identifiers = {oid: case_note for oid in group["ORDER_ID"]}
            log.info("Pattern '%s' — %d orders", pattern, len(identifiers))
            batch_confirm(args.env, args.dsid, token, identifiers, case_note)

            total_actioned += len(identifiers)
            pattern_count  += 1

        post_success_to_slack(total_actioned, pattern_count)

    except Exception:
        log.exception("Run failed — NO Slack post (per design)")
        raise
    finally:
        log.info("═══ Finished in %.1fs ═══", (datetime.now() - start).total_seconds())


if __name__ == "__main__":
    main()