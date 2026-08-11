import re
from datetime import datetime, timedelta

with open("/Users/trevorjosephallen/Documents/trader_notifications.txt", 'r') as f:
    content = f.read()

times = re.findall(r'TFA Trader Notification\s+\[(\d{1,2}:\d{2}\s*[AP]M)\]', content)

cur = datetime(2026, 1, 5)
dates_seen = set()
dates_seen.add(cur.strftime("%Y-%m-%d"))

for i in range(1, len(times)):
    h = datetime.strptime(times[i].strip(), "%I:%M %p").hour
    prev_h = datetime.strptime(times[i-1].strip(), "%I:%M %p").hour
    if prev_h >= 12 and h < 12:
        cur += timedelta(days=1)
    dates_seen.add(cur.strftime("%Y-%m-%d"))

# Check which dates are missing from Jan 5 to Feb 5
d = datetime(2026, 1, 5)
while d <= datetime(2026, 2, 5):
    status = "✓" if d.strftime("%Y-%m-%d") in dates_seen else "MISSING"
    print(f"  {d.strftime('%Y-%m-%d %A'):<25} {status}")
    d += timedelta(days=1)

