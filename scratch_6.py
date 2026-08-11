outfile = "/Users/trevorjosephallen/Documents/trader_parsed.csv"
with open(outfile, 'w', newline='') as f:
    f.write("date,time,trader_genuine_customer_orders,trader_good_product_set,AMR_gibberish_em_a1add_velocity,TRADER_5600_BINS_370021_373778,TRADER_AMR_Common_EM_DOM,TRADER_AMR_UNIQUE_EM_DOM,total,runtime,gct,th\n")
    for e in entries:
        f.write(f"{e['date']},{e['time']},{e['trader_genuine_customer_orders']},{e['trader_good_product_set']},{e['AMR_gibberish_em_a1add_velocity']},{e['TRADER_5600_BINS_370021_373778']},{e['TRADER_AMR_Common_EM_DOM']},{e['TRADER_AMR_UNIQUE_EM_DOM']},{e['total']},{e['runtime']},{e['gct']},{e['th']}\n")
