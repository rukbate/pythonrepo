from datetime import date
from datetime import timedelta
import mdb

def getStartDate(exchange, code):
    maxDateSql = "select max(date) as max_date from day_k where exchange = '{exchange}' and code = '{code}'"
    conn = mdb.connectAShare()

    rs = mdb.query(conn, maxDateSql.format(exchange = exchange, code = code))
    conn.close()
    
    maxDate = rs.loc[0, 'max_date']
    if maxDate != None:
        startDate = maxDate + timedelta(days=1)
    else:
        startDate = date(1980, 1, 1)

    return startDate

def getEndDate():
    return date.today() + timedelta(days=1)