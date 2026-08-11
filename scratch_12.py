import re
from datetime import datetime, timedelta

with open("/Users/trevorjosephallen/Documents/trader_notifications.txt", 'r') as f:
    content = f.read()

times = re.findall(r'TFA Trader Notification\s+\[(\d{1,2}:\d{2}\s*[AP]M)\]', content)

# Work backwards from Feb 5
cur = datetime(2026, 2, 5)
dates = [None] * len(times)
dates[-1] = cur

for i in range(len(times) - 2, -1, -1):
    h = datetime.strptime(times[i].strip(), "%I:%M %p").hour
    next_h = datetime.strptime(times[i+1].strip(), "%I:%M %p").hour
    # If current is PM and next is AM, next is a new day, so current is previous day
    if h >= 12 and next_h < 12:
        cur -= timedelta(days=1)
    dates[i] = cur

for i, t in enumerate(times):
    print(f"{i:<4} {dates[i].strftime('%Y-%m-%d')}  {t.strip()}")

print(f"\nStart: {dates[0].strftime('%Y-%m-%d')}")
print(f"End:   {dates[-1].strftime('%Y-%m-%d')}")
