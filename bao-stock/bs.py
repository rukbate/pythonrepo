import baostock as bs
import pandas as pd

def bsLogin():
    lg = bs.login()
    print('login respond error_code:' + lg.error_code)
    print('login respond  error_msg:' + lg.error_msg)


def bsLogout():
    bs.logout()

def bsQueryDailyKData(code, startDate, endDate):
    rs = bs.query_history_k_data_plus(
        code,
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
        start_date=startDate, end_date=endDate,
        frequency="d", adjustflag="3")

    print('bsQueryDailyKData respond error_code:' + rs.error_code)
    print('bsQueryDailyKData respond error_msg:' + rs.error_msg)

    resultset = []
    while(rs.error_code == '0') & rs.next():
        row = rs.get_row_data()
        resultset.append(row)
    
    return pd.DataFrame(resultset, columns=rs.fields)
