import re, csv, sys
from datetime import datetime, timedelta
from collections import defaultdict


def parse(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    headers = list(re.finditer(r'TFA Trader Notification\s+\[(\d{1,2}:\d{2}\s*[AP]M)\]', content))

    # DEBUG: show what we found
    print(f"Found {len(headers)} notification headers")
    for i, m in enumerate(headers[:5]):
        print(f"  [{i}] raw time: '{m.group(1)}'")

    entries = []
    pm_to_am_count = 0

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

    # Assign dates using a calendar starting Jan 5 2026
    # Rule: every time we go from PM to AM = new day
    cur = datetime(2026, 1, 5)

    for i, e in enumerate(entries):
        t = datetime.strptime(e['time'], "%I:%M %p")
        is_am = t.hour < 12

        if i > 0:
            prev_t = datetime.strptime(entries[i - 1]['time'], "%I:%M %p")
            prev_is_am = prev_t.hour < 12

            if not prev_is_am and is_am:  # PM -> AM = new day
                cur += timedelta(days=1)
                pm_to_am_count += 1

        e['date'] = cur.strftime("%Y-%m-%d")

    print(f"\nPM->AM transitions (new days): {pm_to_am_count}")
    print(f"Date range: {entries[0]['date']} to {entries[-1]['date']}")
    print(f"Total days: {pm_to_am_count + 1}")
    print(f"\nFirst 15 entries:")
    for e in entries[:15]:
        print(f"  {e['date']}  {e['time']:<10}  TH={e['th']:>5,}")
    print(f"\nLast 5 entries:")
    for e in entries[-5:]:
        print(f"  {e['date']}  {e['time']:<10}  TH={e['th']:>5,}")

    return entries


def main():
    infile = sys.argv[1] if len(sys.argv) > 1 else "/Users/trevorjosephallen/Documents/trader_notifications.txt"
    outfile = "/Users/trevorjosephallen/Documents/trader_parsed.csv"

    entries = parse(infile)

    cols = ['date', 'time', 'trader_genuine_customer_orders', 'trader_good_product_set',
            'AMR_gibberish_em_a1add_velocity', 'TRADER_5600_BINS_370021_373778',
            'TRADER_AMR_Common_EM_DOM', 'TRADER_AMR_UNIQUE_EM_DOM', 'total', 'runtime', 'gct', 'th']
    with open(outfile, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(entries)

    daily = defaultdict(lambda: {'th': 0, 'gct': 0})
    for e in entries:
        daily[e['date']]['th'] += e['th']
        daily[e['date']]['gct'] += e['gct']

    print(f"\nWrote {len(entries)} entries -> {outfile}\n")
    grand_th = 0
    for d in sorted(daily):
        print(f"  {d}  GCT={daily[d]['gct']:>6,}  TH={daily[d]['th']:>7,}")
        grand_th += daily[d]['th']
    print(f"\n  GRAND TOTAL TH: {grand_th:,}")


if __name__ == "__main__":
    main()
