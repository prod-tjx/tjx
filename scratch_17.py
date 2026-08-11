import re

# Read the file
with open("/Users/trevorjosephallen/Documents/th5678.txt", 'r') as f:
    content = f.read()

# Find all TH values
th_values = re.findall(r'TH:\s*(\d+)', content)

# Convert to integers and sum
th_numbers = [int(x) for x in th_values]
total = sum(th_numbers)

# Print result
print(f"Total TH: {total:,}")
print(f"Number of entries: {len(th_numbers)}")
