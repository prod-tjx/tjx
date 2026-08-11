import re
from datetime import datetime, timedelta
import pandas as pd

# Read the file
file_path = "/Users/trevorjosephallen/Documents/th5678.txt"
print(f"Reading file: {file_path}")

with open(file_path, 'r') as f:
    content = f.read()

print(f"File length: {len(content)} characters")

# Calculate average value per action - EXACT
total_value = 3447332438.22
total_actions = 1710832
avg_value_per_action = total_value / total_actions

# Split by date headers
notifications = re.split(r'tfa-trader-hunting\n', content)
print(f"Found {len(notifications)} notification chunks")

run_data = []
entry_num = 1
grand_total_th = 0
grand_total_gct = 0
grand_total_orders = 0
grand_total_value = 0

for i, notification in enumerate(notifications):
    if not notification.strip():
        continue

    if 'Trader Alerts Results' not in notification:
        continue

    # Extract date and time - handle multiple formats
    datetime_match = re.search(r'(\w+ \d+)\w* at (\d{2}:\d{2})', notification)
    today_match = re.search(r'Today at (\d{2}:\d{2})', notification)
    yesterday_match = re.search(r'Yesterday at (\d{2}:\d{2})', notification)

    if datetime_match:
        date_str = datetime_match.group(1)  # Jan 5
        time_str = datetime_match.group(2)  # 09:37
    elif today_match:
        date_str = "Feb 5"  # Today = Feb 5, 2026
        time_str = today_match.group(1)
    elif yesterday_match:
        date_str = "Feb 4"  # Yesterday = Feb 4, 2026
        time_str = yesterday_match.group(1)
    else:
        continue

    # Extract values
    th_match = re.search(r'TH:\s*(\d+)', notification)
    gct_match = re.search(r'GCT:\s*(\d+)', notification)
    total_match = re.search(r'Total:\s*(\d+)\s*orders', notification)
    runtime_match = re.search(r'Runtime:\s*([\d.]+)s', notification)

    if not th_match:
        continue

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

    run_data.append([
        entry_num,
        f"{date_str}, {time_str}",
        th,
        gct,
        total,
        runtime,
        ', '.join(queries),
        f"${est_tfa_value:,.2f}"
    ])

    entry_num += 1

print(f"Processed {len(run_data)} runs")

# Check if we have data
if len(run_data) == 0:
    print("ERROR: No data found!")
    exit()

# Create DataFrame
df_runs = pd.DataFrame(run_data, columns=['#', 'Date/Time', 'TH', 'GCT', 'Total', 'Runtime (s)', 'Queries Active',
                                          'Est. TFA Value'])

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
for row in run_data:
    date_part = row[1].split(',')[0]
    if date_part not in daily_summary:
        daily_summary[date_part] = {'th': 0, 'gct': 0, 'total': 0, 'runs': 0}
    daily_summary[date_part]['th'] += row[2]
    daily_summary[date_part]['gct'] += row[3]
    daily_summary[date_part]['total'] += row[4]
    daily_summary[date_part]['runs'] += 1

daily_data = []
for date, vals in daily_summary.items():
    est_value = vals['th'] * avg_value_per_action
    daily_data.append([
        date,
        vals['runs'],
        vals['th'],
        vals['gct'],
        vals['total'],
        f"${est_value:,.2f}"
    ])

df_daily = pd.DataFrame(daily_data, columns=['Date', 'Runs', 'TH', 'GCT', 'Total Orders', 'Est. TFA Value'])

# Write to Excel
output_path = "/Users/trevorjosephallen/Documents/th_breakdown.xlsx"
with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    df_runs.to_excel(writer, sheet_name='Run by Run Breakdown', index=False)
    df_fiscal.to_excel(writer, sheet_name='Fiscal Impact Estimate', index=False)
    df_daily.to_excel(writer, sheet_name='Daily Summary', index=False)

print(f"")
print(f"Done! Excel saved to: {output_path}")
print(f"")
print(f"=== FINAL TOTALS ===")
print(f"Total Runs: {len(run_data)}")
print(f"Grand Total TH: {grand_total_th:,}")
print(f"Grand Total GCT: {grand_total_gct:,}")
print(f"Grand Total Orders: {grand_total_orders:,}")
print(f"Grand Total Est. Value: ${grand_total_value:,.2f}")
