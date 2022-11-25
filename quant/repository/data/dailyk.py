import threading
from datetime import date
from datetime import timedelta

import pandas as pd
from market import market
from repository import mdb
from repository.data import basic
from repository.data import index
from util.stringutil import normalize_number as normalize


def existing_stocks():
    sql = "select distinct code from dailyk"
    conn = mdb.connect_db()

    rs = mdb.query(conn, sql)
    mdb.close(conn)

    return rs


def persist_daily_k(data):
    sql = """insert into dailyk(code, date, open, high, low, close, pre_close, volume, 
             amount, adjust_flag, turn, trade_status, pct_chg, pe_ttm, pb_mrq, ps_ttm, pcf_nc_ttm, is_st)
             values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          """

    kdata = [(
        row['code'],
        row['date'],
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
        normalize(row['isST'])
    ) for index, row in data.iterrows()]
    conn = mdb.connect_db()
    for curr in range(0, len(kdata), 1000):
        mdb.execute_batch(conn, sql, kdata[curr:min(curr + 1000, len(kdata))])
    mdb.close(conn)


def sync_daily_k(code):
    print(f"Starting to sync daily k for {code}")
    start_date = latest_date(code)
    end_date = date.today() + timedelta(days=1)

    if start_date == date.today():
        print(f"{code} already up-to-date, skip syncing...")
        return
    else:
        print(f"Syncing daily k for {code} from {start_date.isoformat()}")

    new_data = market.daily_k(code, start_date.isoformat(), end_date.isoformat())
    print(f"Retrieved {len(new_data)} days data for {code}")

    persist_daily_k(new_data)


def sync_stocks(codes):
    for code in codes:
        sync_daily_k(code)


def sync_all_stocks():
    stocks = market.all_stocks((date.today() + timedelta(days=-1)).isoformat())
    codes = []
    for code, status, name in stocks.to_numpy():
        if code[:2] != 'bj':
            codes.append(code)

    t1 = threading.Thread(target=sync_stocks, args=(codes[:],))
    t1.start()
    t1.join()


def sync_index_stocks():
    codes = []
    for idx in ['sz50', 'hs300', 'zz500']:
        stocks = index.retrieve_ndex(idx)
        for s in stocks['code']:
            codes.append(s)

    sync_stocks(codes)


def retrieve_daily_k(code, start_date=None, end_date=None):
    sql = f"select date, open, close, high, low, volume from dailyk where code = '{code}'"
    if start_date is not None:
        sql = sql + f" and date >= '{start_date}'"
    if end_date is not None:
        sql = sql + f" and date <= '{end_date}'"
    sql = sql + " order by date"
    conn = mdb.connect_db()
    data = mdb.query(conn, sql)

    mdb.close(conn)
    data.index = pd.to_datetime(data['date'])
    return data


def latest_date(code):
    max_date_sql = "select max(date) as max_date from dailyk where code = '{code}'"
    conn = mdb.connect_db()

    rs = mdb.query(conn, max_date_sql.format(code=code))
    conn.close()

    max_date = rs.loc[0, 'max_date']
    if max_date is not None:
        start_date = max_date + timedelta(days=1)
    else:
        start_date = basic.ipo_date(code)
        if start_date is None:
            start_date = date(1980, 1, 1)

    return start_date
