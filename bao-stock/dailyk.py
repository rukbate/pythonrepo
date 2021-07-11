import bao
import mdb
import datetool as dt
from datetime import date
import pandas as pd

def queryDailyK(exchange, code, startDate, endDate):
    data = bao.queryDailyKData(exchange + '.' + code, startDate, endDate)
    return data

def getExistingStocks():
    sql = "select distinct exchange, code from day_k"
    conn = mdb.connectAShare()

    rs = mdb.query(conn, sql)
    mdb.close(conn)

    return rs

def normalize(val):
    return val if len(val) > 0 else '0'

def persistDailyK(data):
    sql = """insert into day_k(exchange, date, code, open, high, low, close, pre_close, volume, 
             amount, adjust_flag, turn, trade_status, pct_chg, pe_ttm, pb_mrq, ps_ttm, pcf_nc_ttm, is_st)
             values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          """

    kdata = []    
    for index, row in data.iterrows():
        kdata.append(
            (
                row['code'][:2],
                row['date'],
                row['code'][3:],
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['preclose'],
                normalize(row['volume']),
                normalize(row['amount']),
                row['adjustflag'],
                normalize(row['turn']),
                row['tradestatus'],
                normalize(row['pctChg']),
                normalize(row['peTTM']),
                normalize(row['pbMRQ']),
                normalize(row['psTTM']),
                normalize(row['pcfNcfTTM']),
                row['isST']
            )
        )

    conn = mdb.connectAShare()
    curr = 0
    while curr < len(kdata):
        mdb.executeMany(conn, sql, kdata[curr:min(curr+1000, len(kdata))])
        curr += 1000

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

def getDailyK(exchange, code):
    conn = mdb.connectAShare()
    data = mdb.query(
        conn, 
        f"select date, open, close, high, low, volume from day_k where exchange = '{exchange}' and code = '{code}' order by date"
    )

    mdb.close(conn)
    data.index = pd.to_datetime(data['date'])
    return data