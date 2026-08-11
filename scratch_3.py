import re, sys
from datetime import datetime, timedelta

with open("/Users/trevorjosephallen/Documents/trader_notifications.txt", 'r') as f:
    content = f.read()

times = re.findall(r'TFA Trader Notification\s+\[(\d{1,2}:\d{2}\s*[AP]M)\]', content)

cur = datetime(2026, 1, 5)
for i, t in enumerate(times):
    h = datetime.strptime(t.strip(), "%I:%M %p").hour
    if i > 0:
        prev_h = datetime.strptime(times[i-1].strip(), "%I:%M %p").hour
        if prev_h >= 12 and h < 12:
            cur += timedelta(days=1)
    print(f"{cur.strftime('%Y-%m-%d')}  {t.strip()}")


