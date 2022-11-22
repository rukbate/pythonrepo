import baostock as bs
import pandas as pd
import sys

__logged_in__ = False

indexes = {
    'sz50': bs.query_sz50_stocks,
    'hs300': bs.query_hs300_stocks,
    'zz500': bs.query_zz500_stocks
}


def login():
    global __logged_in__
    lg = bs.login()
    __logged_in__ = True

    if lg.error_code != '0':
        print(f'login error_code: {lg.error_code}, error_msg: {lg.error_msg}')
        sys.exit(1)


def logout():
    global __logged_in__
    bs.logout()
    __logged_in__ = False


def enforce_login():
    global __logged_in__
    if not __logged_in__:
        login()
        __logged_in__ = True


def daily_k(code, start_date, end_date):
    enforce_login()

    rs = bs.query_history_k_data_plus(
        code,
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
        start_date=start_date, end_date=end_date,
        frequency="d", adjustflag="3")

    if rs.error_code != '0':
        print(f'query_history_k_data_plus error_code: {rs.error_code}, error_msg: {rs.error_msg}')
        sys.exit(1)

    data = []
    while (rs.error_code == '0') & rs.next():
        data.append(rs.get_row_data())

    return pd.DataFrame(data, columns=rs.fields)


def index_stocks(idx):
    if idx not in indexes:
        print(f"Invalid index type: {idx}")
        return

    enforce_login()

    rs = indexes[idx]()
    if rs.error_code != '0':
        print(f'query {idx} error_code: {rs.error_code}, error_msg: {rs.error_msg}')
        sys.exit(1)

    stocks = []
    while (rs.error_code == '0') & rs.next():
        stocks.append(rs.get_row_data())

    return pd.DataFrame(stocks, columns=rs.fields)


def stock_basic(exchange, code):
    enforce_login()

    rs = bs.query_stock_basic(code=f"{exchange}.{code}")
    if rs.error_code != '0':
        print(f'query_stock_basic error_code: {rs.error_code}, error_msg: {rs.error_msg}')
        sys.exit(1)

    data = []
    while (rs.error_code == '0') & rs.next():
        data.append(rs.get_row_data())

    return pd.DataFrame(data, columns=rs.fields)
