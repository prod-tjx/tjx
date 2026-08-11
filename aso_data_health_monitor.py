"""
ASO Transactions Data Health Monitor
Monitors data freshness and alerts on delays
"""

import os
import sys
import json
from datetime import datetime
from ai.snowflake.compat.connection import sf

# Notification thresholds (in seconds)
MIN_DELAY_TO_NOTIFY = 900  # 15 minutes - don't notify below this
DELAY_30MIN = 1800  # 30 minutes

# Slack webhook
SLACK_WEBHOOK = os.getenv('SLACK_WEBHOOK_URL', '')


def get_aso_delay():
    """Get the latest ASO transaction timestamp and calculate delay"""
    query = """
            SELECT MAX(event_ts)                                             AS max_utc_ts,
                   CONVERT_TIMEZONE('UTC', 'America/Chicago', MAX(event_ts)) AS max_cst_ts,
                   DATEDIFF('second', MAX(event_ts), CURRENT_TIMESTAMP)      AS delay_seconds,
                   CONCAT(
                           FLOOR(DATEDIFF('second', MAX(event_ts), CURRENT_TIMESTAMP) / 60),
                           'm ',
                           MOD(DATEDIFF('second', MAX(event_ts), CURRENT_TIMESTAMP), 60),
                           's'
                   )                                                         AS delay_formatted
            FROM gbi_fraud_bap_db.ai_live_biz_app.aso_transactions
            WHERE event_ts >= CURRENT_TIMESTAMP - INTERVAL '4 hours';
            """

    try:
        df = sf.run_query(query)
        if df.empty:
            return None

        result = {
            'max_utc_ts': str(df['max_utc_ts'].iloc[0]),
            'max_cst_ts': str(df['max_cst_ts'].iloc[0]),
            'delay_seconds': int(df['delay_seconds'].iloc[0]),
            'delay_formatted': str(df['delay_formatted'].iloc[0]),
            'check_time': datetime.utcnow().isoformat()
        }

        return result

    except Exception as e:
        print(f"❌ Query failed: {str(e)}")
        return None


def save_results(result):
    """Save results to JSON"""
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    filename = f'aso_health_check_{timestamp}.json'

    with open(filename, 'w') as f:
        json.dump(result, f, indent=2)

    with open('aso_health_check_latest.json', 'w') as f:
        json.dump(result, f, indent=2)

    return filename


def format_slack_payload(result):
    """Format Slack payload with colored attachment"""
    delay_seconds = result['delay_seconds']
    
    # Only create payload if delay >= 10 minutes
    if delay_seconds < MIN_DELAY_TO_NOTIFY:
        return None
    
    delay_formatted = result['delay_formatted']
    max_cst_ts = result['max_cst_ts']
    
    # Determine color and message based on delay
    if delay_seconds >= DELAY_30MIN:
        color = 'danger'  # RED for 30+ minutes
        message = f"⚠️ ASO Transactions Delay\n\nLatest timestamp *{delay_formatted}* behind\n\nLatest Event: `{max_cst_ts} CST`"
    else:
        color = 'warning'  # YELLOW for 10-30 minutes
        message = f"⚠️ Possible ASO Transactions Data Delay\n\nLatest timestamp *{delay_formatted}* behind\n\nLatest Event: `{max_cst_ts} CST`"
    
    payload = {
        "text": message,
        "attachments": [{
            "color": color,
            "fields": [
                {
                    "title": "Delay",
                    "value": delay_formatted,
                    "short": True
                },
                {
                    "title": "Latest Event (CST)",
                    "value": max_cst_ts,
                    "short": True
                }
            ],
            "footer": "ASO_Transactions Data Health Monitor"
        }]
    }
    
    return payload


def send_slack_notification(payload):
    """Send notification to Slack"""
    if not SLACK_WEBHOOK:
        print("⚠️ No Slack webhook configured")
        return False

    try:
        import requests

        response = requests.post(
            SLACK_WEBHOOK,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )

        if response.status_code == 200:
            print(f"✅ Slack notification sent")
            return True
        else:
            print(f"❌ Slack notification failed: {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ Failed to send Slack notification: {str(e)}")
        return False


def main():
    print("ASO TRANSACTIONS DATA HEALTH MONITOR")
    print("=" * 60)

    result = get_aso_delay()

    if not result:
        print("❌ Failed to retrieve ASO delay information")
        sys.exit(1)

    delay_seconds = result['delay_seconds']
    delay_formatted = result['delay_formatted']

    print(f"\nLatest Event (CST): {result['max_cst_ts']}")
    print(f"Delay: {delay_formatted} ({delay_seconds} seconds)")

    save_results(result)
    
    # Send notification if delay > 10 minutes
    if delay_seconds >= MIN_DELAY_TO_NOTIFY:
        print(f"\nDelay exceeds {MIN_DELAY_TO_NOTIFY // 60}min, sending notification...")
        slack_payload = format_slack_payload(result)
        
        if slack_payload:
            send_slack_notification(slack_payload)
    else:
        print(f"\nDelay under {MIN_DELAY_TO_NOTIFY // 60}min, no notification needed")

    print("=" * 60)
    print("✅ Health check completed")


if __name__ == "__main__":
    main()
