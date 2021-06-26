import baostock as bs
import pandas as pd
import sys

__logged_in__ = False

def login():
    global __logged_in__
    lg = bs.login()
    __logged_in__ = True

    if(lg.error_code != '0'):
        print(f'Login error_code: {lg.error_code}, error_msg: {lg.error_msg}')
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
        print(f'bsQueryDailyKData error_code: {rs.error_code}, error_msg: {rs.error_msg}')
        sys.exit(1)

    resultset = []
    while(rs.error_code == '0') & rs.next():
        row = rs.get_row_data()
        resultset.append(row)
    
    return pd.DataFrame(resultset, columns=rs.fields)
