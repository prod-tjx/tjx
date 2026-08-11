with open("/Users/trevorjosephallen/Documents/trader_notifications.txt", 'r') as f:
    content = f.read()

import re
times = re.findall(r'\[(\d{1,2}:\d{2}\s*[AP]M)\]', content)
for i, t in enumerate(times):
    print(f"{i}: {t}")
