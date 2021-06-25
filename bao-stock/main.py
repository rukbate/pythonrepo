import bs
import mdb

conn = mdb.dbConnect("192.168.11.11", 3307, "ashare", "synology@mariaDB10", "ashare")

# cur.execute("insert into stock(code, name) values('000004', 'stock 4')")
# conn.commit()

sql = """
    insert into day_k(market, date, code, open, high, low, close, pre_close, volume, amount, adjust_flag, turn, trade_status, pct_chg, pe_ttm, pb_mrq, ps_ttm, pcf_nc_ttm, is_st)
    values('{market}', '{date}', '{code}', {open}, {high}, {low}, {close}, {preclose}, {volume}, {amount}, {adjustflag}, {turn}, {tradestatus}, {pctChg}, {peTTM}, {pbMRQ}, {psTTM}, {pcfNcfTTM}, {isST})
    """
# mdb.dbExecute(conn, sql)



bs.bsLogin()

data = bs.bsQueryDailyKData("sh.601888", "2000-01-01", "2021-12-31")
bs.bsLogout()

for index, row in data.iterrows():
    row['market'] = row['code'][:2]
    row['code'] = row['code'][3:]
    row['turn'] = row['turn'] if len(row['turn']) > 0 else '0'
    # print(sql.format(**row))
    mdb.dbExecute(conn, sql.format(**row))

print(f"Total: {len(data)}")

# cur = mdb.dbQuery(conn, "select * from day_k")
# for(market, date, code, open, high, low, close, pre_close, volume, amount, adjustflag, turn, tradestatus, pct_chg, pe_ttm, pb_mrq, ps_ttm, pcf_nc_ttm, is_st) in cur:
#     print(f"Code: {code}, Date: {date}")

mdb.dbClose(conn)