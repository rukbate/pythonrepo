import bao
import mdb
import datetool as dt
from datetime import date

def queryDailyK(exchange, code, startDate, endDate):
    data = bao.queryDailyKData(exchange + '.' + code, startDate, endDate)

    return data

def getExistingStocks():
    sql = "select distinct exchange, code from day_k"
    conn = mdb.connectAShare()

    rs = mdb.query(conn, sql)
    mdb.close(conn)

    return rs

def persistDailyK(data):
    sql = """
        insert into day_k(exchange, date, code, open, high, low, close, pre_close, volume, amount, adjust_flag, turn, trade_status, pct_chg, pe_ttm, pb_mrq, ps_ttm, pcf_nc_ttm, is_st)
        values('{exchange}', '{date}', '{code}', {open}, {high}, {low}, {close}, {preclose}, {volume}, {amount}, {adjustflag}, {turn}, {tradestatus}, {pctChg}, {peTTM}, {pbMRQ}, {psTTM}, {pcfNcfTTM}, {isST})
        """

    conn = mdb.connectAShare()
    for index, row in data.iterrows():
        row['exchange'] = row['code'][:2]
        row['code'] = row['code'][3:]
        row['turn'] = row['turn'] if len(row['turn']) > 0 else '0'
        mdb.execute(conn, sql.format(**row))
    
    mdb.close(conn)

def syncDailyK(exchange, code):
    startDate = dt.getStartDate(exchange, code)
    endDate = dt.getEndDate()

    if(startDate == date.today()):
        print(f"{exchange}.{code} already up-to-date, skip syncing...")
        return
    else:
        print(f"Syncing daily k for {exchange}.{code} from {startDate.isoformat()}")

    newData = queryDailyK(exchange, code, startDate.isoformat(), endDate.isoformat())
    print(f"Retrieved {len(newData)} days data for {exchange}.{code}")

    persistDailyK(newData)

def syncExistingStocks():
    stocks = getExistingStocks()

    for exchange, code in stocks.to_numpy():
        syncDailyK(exchange, code)

    


    