import re
from datetime import datetime, timedelta

# Read the file
with open("th234.txt", 'r') as f:
    content = f.read()

# Starting date - January 5th, 2026
current_date = datetime(2026, 1, 5)

# Split into lines
lines = content.split('\n')
output_lines = []

# Add first date header
output_lines.append("=" * 60)
output_lines.append(f"DATE: {current_date.strftime('%B %d, %Y (%A)')}")
output_lines.append("=" * 60)
output_lines.append("")

for line in lines:
    output_lines.append(line)

    # After 11:XX PM, add next day's date
    if re.search(r'\[11:\d{2}\s*PM\]', line):
        current_date += timedelta(days=1)
        output_lines.append("")
        output_lines.append("=" * 60)
        output_lines.append(f"DATE: {current_date.strftime('%B %d, %Y (%A)')}")
        output_lines.append("=" * 60)
        output_lines.append("")

# Save output
with open("th234_dated.txt", 'w') as f:
    f.write('\n'.join(output_lines))

print("Done! Check th234_dated.txt")
