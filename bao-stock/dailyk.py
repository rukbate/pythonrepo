import bao
import mdb
import datetool as dt
from datetime import date

def queryDailyK(market, code, startDate, endDate):
    bao.login()

    data = bao.queryDailyKData(market + '.' + code, startDate, endDate)
    bao.logout()

    return data

def getExistingStocks():
    sql = "select distinct market, code from day_k"
    conn = mdb.connectAShare()

    rs = mdb.query(conn, sql)
    mdb.close(conn)

    return rs

def saveDailyK(data):
    sql = """
        insert into day_k(market, date, code, open, high, low, close, pre_close, volume, amount, adjust_flag, turn, trade_status, pct_chg, pe_ttm, pb_mrq, ps_ttm, pcf_nc_ttm, is_st)
        values('{market}', '{date}', '{code}', {open}, {high}, {low}, {close}, {preclose}, {volume}, {amount}, {adjustflag}, {turn}, {tradestatus}, {pctChg}, {peTTM}, {pbMRQ}, {psTTM}, {pcfNcfTTM}, {isST})
        """

    conn = mdb.connectAShare()
    for index, row in data.iterrows():
        row['market'] = row['code'][:2]
        row['code'] = row['code'][3:]
        row['turn'] = row['turn'] if len(row['turn']) > 0 else '0'
        mdb.execute(conn, sql.format(**row))
    
    mdb.close(conn)

def syncDailyK(market, code):
    startDate = dt.getStartDate(market, code)
    endDate = dt.getEndDate()

    if(startDate == date.today()):
        print(f"{market}.{code} already up-to-date, skip syncing...")
        return
    else:
        print(f"Syncing daily k for {market}.{code} from {startDate.isoformat()}")

    newData = queryDailyK(market, code, startDate.isoformat(), endDate.isoformat())
    print(f"Retrieved {len(newData)} days data for {market}.{code}")

    saveDailyK(newData)

def syncExistingStocks():
    stocks = getExistingStocks()

    for market, code in stocks.to_numpy():
        syncDailyK(market, code)

    


    