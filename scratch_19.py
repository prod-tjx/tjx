import re
from datetime import datetime, timedelta
import pandas as pd

# Read the file
with open("/Users/trevorjosephallen/Documents/th5678.txt", 'r') as f:
    content = f.read()

# Calculate average value per action - EXACT
total_value = 3447332438.22
total_actions = 1710832
avg_value_per_action = total_value / total_actions

# Split by notifications
notifications = re.split(r'TFA Trader Notification\s*\[', content)

# Starting date
current_date = datetime(2026, 1, 5)
run_data = []
entry_num = 1
grand_total_th = 0
grand_total_gct = 0
grand_total_orders = 0
grand_total_value = 0

for notification in notifications:
    if not notification.strip() or 'Trader Alerts Results' not in notification:
        continue

    # Extract time
    time_match = re.match(r'(\d{1,2}:\d{2}\s*[AP]M)\]', notification)
    if not time_match:
        continue
    time_str = time_match.group(1)

    # Extract values
    th_match = re.search(r'TH:\s*(\d+)', notification)
    gct_match = re.search(r'GCT:\s*(\d+)', notification)
    total_match = re.search(r'Total:\s*(\d+)\s*orders', notification)
    runtime_match = re.search(r'Runtime:\s*([\d.]+)s', notification)

    # Extract query types
    queries = []
    if 'gibberish' in notification:
        queries.append('gibberish')
    if '5600_BINS' in notification:
        queries.append('5600_BINS')
    if 'Common_EM' in notification:
        queries.append('Common_EM')
    if 'UNIQUE_EM' in notification:
        queries.append('UNIQUE_EM')

    th = int(th_match.group(1)) if th_match else 0
    gct = int(gct_match.group(1)) if gct_match else 0
    total = int(total_match.group(1)) if total_match else 0
    runtime = float(runtime_match.group(1)) if runtime_match else 0

    # Calculate estimated TFA value - EXACT PRECISION
    est_tfa_value = th * avg_value_per_action

    grand_total_th += th
    grand_total_gct += gct
    grand_total_orders += total
    grand_total_value += est_tfa_value

    # Format date
    date_str = current_date.strftime('%b %d')

    run_data.append({
        'Num': entry_num,
        'DateTime': f"{date_str}, {time_str}",
        'TH': th,
        'GCT': gct,
        'Total': total,
        'Runtime': runtime,
        'Queries': ', '.join(queries),
        'EstValue': f"${est_tfa_value:,.2f}"
    })

    entry_num += 1

    # Increment date after 11 PM
    if '11:' in time_str and 'PM' in time_str:
        current_date += timedelta(days=1)

# Create DataFrame
df_runs = pd.DataFrame(run_data)
df_runs.columns = ['#', 'Date/Time', 'TH', 'GCT', 'Total', 'Runtime (s)', 'Queries Active', 'Est. TFA Value']

# Sheet 2: Fiscal Impact Estimate
fiscal_data = {
    'Metric': [
        'Report Period',
        'Total Runs',
        '',
        'DETECTION METRICS',
        'Total TH (Trader Hunting) Orders',
        'Total GCT (Genuine Customer) Orders',
        'Total Orders Processed',
        '',
        'VALUE CALCULATION',
        'Historical Total Value',
        'Historical Total Actions',
        'Average Value Per Action',
        '',
        'FISCAL IMPACT ESTIMATE',
        'Estimated TFA Value (TH x Avg Value)',
        '',
        'DISCLAIMER',
        'This is an ESTIMATE based on historical averages.',
        'TFA labels are not consistently tracked.',
        'A SQL pull would provide accurate data:',
        'SELECT * FROM gbi_fraud_bap_db.ai_live_biz_app.Trader_Automated_Queries_Results',
        'Retention mechanism for tracking results has not been built.',
        'Values are SPECULATIVE.'
    ],
    'Value': [
        'Jan 5, 2026 - Feb 5, 2026',
        len(run_data),
        '',
        '',
        f"{grand_total_th:,}",
        f"{grand_total_gct:,}",
        f"{grand_total_orders:,}",
        '',
        '',
        f"${total_value:,.2f}",
        f"{total_actions:,}",
        f"${avg_value_per_action:,.10f}",
        '',
        '',
        f"${grand_total_value:,.2f}",
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        ''
    ]
}

df_fiscal = pd.DataFrame(fiscal_data)

# Sheet 3: Daily Summary
daily_summary = {}
current_date = datetime(2026, 1, 5)
for notification in notifications:
    if not notification.strip() or 'Trader Alerts Results' not in notification:
        continue

    time_match = re.match(r'(\d{1,2}:\d{2}\s*[AP]M)\]', notification)
    if not time_match:
        continue
    time_str = time_match.group(1)

    th_match = re.search(r'TH:\s*(\d+)', notification)
    gct_match = re.search(r'GCT:\s*(\d+)', notification)
    total_match = re.search(r'Total:\s*(\d+)\s*orders', notification)

    th = int(th_match.group(1)) if th_match else 0
    gct = int(gct_match.group(1)) if gct_match else 0
    total = int(total_match.group(1)) if total_match else 0

    date_key = current_date.strftime('%Y-%m-%d')
    if date_key not in daily_summary:
        daily_summary[date_key] = {'th': 0, 'gct': 0, 'total': 0, 'runs': 0}

    daily_summary[date_key]['th'] += th
    daily_summary[date_key]['gct'] += gct
    daily_summary[date_key]['total'] += total
    daily_summary[date_key]['runs'] += 1

    if '11:' in time_str and 'PM' in time_str:
        current_date += timedelta(days=1)

daily_data = []
for date, vals in sorted(daily_summary.items()):
    est_value = vals['th'] * avg_value_per_action
    daily_data.append({
        'Date': date,
        'Runs': vals['runs'],
        'TH': vals['th'],
        'GCT': vals['gct'],
        'Total Orders': vals['total'],
        'Est. TFA Value': f"${est_value:,.2f}"
    })

df_daily = pd.DataFrame(daily_data)

# Write to Excel with multiple sheets
output_path = "/Users/trevorjosephallen/Documents/th_breakdown.xlsx"
with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    df_runs.to_excel(writer, sheet_name='Run by Run Breakdown', index=False)
    df_fiscal.to_excel(writer, sheet_name='Fiscal Impact Estimate', index=False)
    df_daily.to_excel(writer, sheet_name='Daily Summary', index=False)

print(f"Done! Excel saved to: {output_path}")
print(f"")
print(f"=== SHEETS CREATED ===")
print(f"1. Run by Run Breakdown ({len(run_data)} entries)")
print(f"2. Fiscal Impact Estimate")
print(f"3. Daily Summary ({len(daily_data)} days)")
print(f"")
print(f"=== EXACT CALCULATIONS ===")
print(f"Total Value: ${total_value:,.2f}")
print(f"Total Actions: {total_actions:,}")
print(f"Avg Value Per Action: ${avg_value_per_action:.10f}")
print(f"")
print(f"Grand Total TH: {grand_total_th:,}")
print(f"Grand Total Est. Value: ${grand_total_value:,.2f}")

