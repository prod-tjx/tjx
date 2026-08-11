import re
from datetime import datetime

with open("/Users/trevorjosephallen/Documents/trader_notifications.txt", 'r') as f:
    content = f.read()

times = re.findall(r'TFA Trader Notification\s+\[(\d{1,2}:\d{2}\s*[AP]M)\]', content)

pm11 = 0
am8 = 0
for t in times:
    h = datetime.strptime(t.strip(), "%I:%M %p").hour
    if h == 23: pm11 += 1
    if h == 8: am8 += 1

print(f"Total entries: {len(times)}")
print(f"11 PM runs: {pm11}")
print(f"8 AM runs: {am8}")
print(f"PM->AM transitions: ", end="")

count = 0
for i in range(1, len(times)):
    h = datetime.strptime(times[i].strip(), "%I:%M %p").hour
    prev_h = datetime.strptime(times[i-1].strip(), "%I:%M %p").hour
    if prev_h >= 12 and h < 12:
        count += 1
print(count)
