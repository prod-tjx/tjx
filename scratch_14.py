with open("/Users/trevorjosephallen/Documents/trader_notifications.txt", 'r') as f:
    content = f.read()

print("LAST 300 CHARS:")
print(repr(content[-300:]))

