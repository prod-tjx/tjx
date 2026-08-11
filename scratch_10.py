import re
from datetime import datetime

with open("/Users/trevorjosephallen/Documents/trader_notifications.txt", 'r') as f:
    content = f.read()

times = re.findall(r'TFA Trader Notification\s+\[(\d{1,2}:\d{2}\s*[AP]M)\]', content)

for i in range(1, len(times)):
    h = datetime.strptime(times[i].strip(), "%I:%M %p").hour
    prev_h = datetime.strptime(times[i-1].strip(), "%I:%M %p").hour
    if prev_h < 12 and h < 12:
        print(f"AM->AM at index {i-1}->{i}: {times[i-1].strip()} -> {times[i].strip()}")
