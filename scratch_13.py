import re
from datetime import datetime, timedelta

with open("/Users/trevorjosephallen/Documents/trader_notifications.txt", 'r') as f:
    content = f.read()

times = re.findall(r'TFA Trader Notification\s+\[(\d{1,2}:\d{2}\s*[AP]M)\]', content)

# Group by PM->AM boundaries (current 31 days)
cur = datetime(2026, 1, 5)
days = {}
day_entries = []

for i, t in enumerate(times):
    t = t.strip()
    h = datetime.strptime(t, "%I:%M %p").hour
    if i > 0:
        prev_h = datetime.strptime(times[i-1].strip(), "%I:%M %p").hour
        if prev_h >= 12 and h < 12:
            days[cur.strftime('%Y-%m-%d')] = day_entries
            cur += timedelta(days=1)
            day_entries = []
    day_entries.append((i, t, h))

days[cur.strftime('%Y-%m-%d')] = day_entries

print(f"{'Date':<14} {'Count':>5}  {'Times'}")
print("-" * 70)
for d in sorted(days):
    t_list = [e[1] for e in days[d]]
    count = len(t_list)
    flag = " <<<" if count > 3 else ""
    print(f"{d:<14} {count:>5}  {', '.join(t_list)}{flag}")

print(f"\nDays with >3 entries need inspection.")
print(f"One of them likely contains 2 calendar days.")
