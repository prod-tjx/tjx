#!/usr/bin/env python3
"""One-shot Slack post — fires the success card with 12.5k orders."""
import requests

WEBHOOK = "https://hooks.slack.com/services/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"

total_orders  = "12.5k"
pattern_count = 3

text = (
    f":white_check_mark: AutoCT actioned *{total_orders}* orders "
    f"across *{pattern_count}* pattern{'s' if pattern_count != 1 else ''}"
)

payload = {
    "text": text,
    "username": "TFA Trader Notification",
    "attachments": [{
        "color": "good",
        "blocks": [
            {"type": "section", "fields": [
                {"type": "mrkdwn", "text": f"*Orders Confirmed*\n{total_orders}"},
                {"type": "mrkdwn", "text": f"*Patterns*\n{pattern_count}"},
            ]},
            {"type": "header", "text": {
                "type": "plain_text",
                "text": "👤 tjx@apple.com |  automating trader tagging since Q4FY25",
                "emoji": True,
            }},
        ],
    }],
}

r = requests.post(WEBHOOK, json=payload, timeout=10)
print(r.status_code, r.text)