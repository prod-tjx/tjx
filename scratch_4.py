import re
from datetime import datetime

with open("/Users/trevorjosephallen/Documents/trader_notifications.txt", 'r') as f:
    content = f.read()

times = re.findall(r'TFA Trader Notification\s+\[(\d{1,2}:\d{2}\s*[AP]M)\]', content)

print(f"Total entries: {len(times)}\n")
print(f"{'#':<4} {'Time':<10} {'AM/PM':<5} {'Transition'}")
print("-" * 40)

for i, t in enumerate(times):
    t = t.strip()
    h = datetime.strptime(t, "%I:%M %p").hour
    ampm = "AM" if h < 12 else "PM"

    transition = ""
    if i > 0:
        prev_h = datetime.strptime(times[i - 1].strip(), "%I:%M %p").hour
        prev_ampm = "AM" if prev_h < 12 else "PM"
        transition = f"{prev_ampm}->{ampm}"
        if prev_ampm == "AM" and ampm == "AM":
            transition += "  *** AM->AM ***"
        if prev_ampm == "PM" and ampm == "AM":
            transition += "  [NEW DAY]"

    print(f"{i:<4} {t:<10} {ampm:<5} {transition}")

