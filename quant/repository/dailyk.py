from market import market
from repository import mdb
from datetime import date
from datetime import timedelta
from repository import info
import pandas as pd


def existing_stocks():
    sql = "select distinct exchange, code from day_k"
    conn = mdb.connect_db()

    rs = mdb.query(conn, sql)
    mdb.close(conn)

    return rs


def normalize(val):
    return val if len(val) > 0 else '0'


def persist_daily_k(data):
    sql = """insert into day_k(exchange, date, code, open, high, low, close, pre_close, volume, 
             amount, adjust_flag, turn, trade_status, pct_chg, pe_ttm, pb_mrq, ps_ttm, pcf_nc_ttm, is_st)
             values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          """

    kdata = [(
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
    ) for index, row in data.iterrows()]
    conn = mdb.connect_db()
    for curr in range(0, len(kdata), 1000):
        mdb.execute_batch(conn, sql, kdata[curr:min(curr + 1000, len(kdata))])
    mdb.close(conn)


def sync_daily_k(exchange, code):
    start_date = latest_date(exchange, code)
    end_date = date.today() + timedelta(days=1)

    if start_date == date.today():
        print(f"{exchange}.{code} already up-to-date, skip syncing...")
        return
    else:
        print(f"Syncing daily k for {exchange}.{code} from {start_date.isoformat()}")

    new_data = market.daily_k(exchange + '.' + code, start_date.isoformat(), end_date.isoformat())
    print(f"Retrieved {len(new_data)} days data for {exchange}.{code}")

    persist_daily_k(new_data)


def sync_all_stocks():
    stocks = existing_stocks()

    for exchange, code in stocks.to_numpy():
        sync_daily_k(exchange, code)


def retrieve_daily_k(exchange, code, start_date=None, end_date=None):
    sql = f"select date, open, close, high, low, volume from day_k where exchange = '{exchange}' and code = '{code}'"
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


def latest_date(exchange, code):
    max_date_sql = "select max(date) as max_date from day_k where exchange = '{exchange}' and code = '{code}'"
    conn = mdb.connect_db()

    rs = mdb.query(conn, max_date_sql.format(exchange=exchange, code=code))
    conn.close()

    max_date = rs.loc[0, 'max_date']
    if max_date is not None:
        start_date = max_date + timedelta(days=1)
    else:
        start_date = info.ipo_date(exchange, code)
        if start_date is None:
            start_date = date(1980, 1, 1)

    return start_date
