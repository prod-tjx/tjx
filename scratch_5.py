import re, csv, sys
from datetime import datetime, timedelta
from collections import defaultdict


def parse(filepath, start="2026-01-05"):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Split on each notification header, grab time
    headers = list(re.finditer(r'TFA Trader Notification\s+\[(\d{1,2}:\d{2}\s*[AP]M)\]', content))
    entries = []

    for i, m in enumerate(headers):
        block = content[m.start(): headers[i + 1].start() if i + 1 < len(headers) else len(content)]
        time_str = m.group(1).strip()
        e = {'time': time_str}

        for key in ['trader_genuine_customer_orders', 'trader_good_product_set',
                    'AMR_gibberish_em_a1add_velocity', 'TRADER_5600_BINS_370021_373778',
                    'TRADER_AMR_Common_EM_DOM', 'TRADER_AMR_UNIQUE_EM_DOM']:
            x = re.search(rf'{key}:\s*(\d+)', block)
            e[key] = int(x.group(1)) if x else 0

        for key, pat in [('total', r'Total:\s*(\d+)'), ('gct', r'GCT:\s*(\d+)'), ('th', r'TH:\s*(\d+)')]:
            x = re.search(pat, block)
            e[key] = int(x.group(1)) if x else 0

        x = re.search(r'Runtime:\s*([\d.]+)', block)
        e['runtime'] = float(x.group(1)) if x else 0.0
        entries.append(e)

    # Assign dates: new day when previous was PM and current is AM
    cur = datetime.strptime(start, "%Y-%m-%d")
    for i, e in enumerate(entries):
        h = datetime.strptime(e['time'], "%I:%M %p").hour
        if i > 0:
            prev_h = datetime.strptime(entries[i - 1]['time'], "%I:%M %p").hour
            if prev_h >= 12 and h < 12:
                cur += timedelta(days=1)
        e['date'] = cur.strftime("%Y-%m-%d")

    return entries


def main():
    infile = sys.argv[1] if len(sys.argv) > 1 else "/Users/trevorjosephallen/Documents/trader_notifications.txt"
    outfile = sys.argv[2] if len(sys.argv) > 2 else "trader_parsed.csv"

    entries = parse(infile)

    # Write CSV
    cols = ['date', 'time', 'trader_genuine_customer_orders', 'trader_good_product_set',
            'AMR_gibberish_em_a1add_velocity', 'TRADER_5600_BINS_370021_373778',
            'TRADER_AMR_Common_EM_DOM', 'TRADER_AMR_UNIQUE_EM_DOM', 'total', 'runtime', 'gct', 'th']
    with open(outfile, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(entries)

    # Summary
    daily = defaultdict(lambda: {'th': 0, 'gct': 0})
    for e in entries:
        daily[e['date']]['th'] += e['th']
        daily[e['date']]['gct'] += e['gct']

    print(f"Parsed {len(entries)} entries -> {outfile}\n")
    grand_th = 0
    for d in sorted(daily):
        print(f"  {d}  GCT={daily[d]['gct']:>6,}  TH={daily[d]['th']:>7,}")
        grand_th += daily[d]['th']
    print(f"\n  GRAND TOTAL TH: {grand_th:,}")


if __name__ == "__main__":
    main()
