import baostock as bs
import pandas as pd
import sys

__logged_in__ = False

def login():
    global __logged_in__
    lg = bs.login()
    __logged_in__ = True

    if(lg.error_code != '0'):
        print(f'login error_code: {lg.error_code}, error_msg: {lg.error_msg}')
        sys.exit(1)

def logout():
    global __logged_in__
    bs.logout()
    __logged_in__ = False

def enforceLogin():
    global __logged_in__
    if not __logged_in__:
        login()
        __logged_in__ = True

def queryDailyKData(code, startDate, endDate):
    enforceLogin()

    rs = bs.query_history_k_data_plus(
        code,
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
        start_date=startDate, end_date=endDate,
        frequency="d", adjustflag="3")

    if(rs.error_code != '0'):
        print(f'query_history_k_data_plus error_code: {rs.error_code}, error_msg: {rs.error_msg}')
        sys.exit(1)

    resultset = []
    while(rs.error_code == '0') & rs.next():
        resultset.append(rs.get_row_data())
    
    return pd.DataFrame(resultset, columns=rs.fields)

def querySZ50Stocks():
    enforceLogin()

    rs = bs.query_sz50_stocks()
    if(rs.error_code != '0'):
        print(f'query_sz50_stocks error_code: {rs.error_code}, error_msg: {rs.error_msg}')
        sys.exit(1)

    sz50_stocks = []
    while (rs.error_code == '0') & rs.next():    
        sz50_stocks.append(rs.get_row_data())

    return pd.DataFrame(sz50_stocks, columns=rs.fields)