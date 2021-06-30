import bao
import mdb

def getInfoFromDb(exchange, code):
    sql = f"select * from info where exchange = '{exchange}' and code = '{code}'"
    conn = mdb.connectAShare()
    data = mdb.query(conn, sql)

    mdb.close(conn)
    return data

def getInfoFromBao(exchange, code):
    return bao.queryStockBasic(exchange, code)

def persistInfo(data):
    sql = """insert into info(exchange, code, name, ipo_date, out_date, type, status)
             values('{exchange}', '{code}', '{code_name}', '{ipoDate}', {outDate}, {type}, {status})
          """

    conn = mdb.connectAShare()
    for index, row in data.iterrows():
        row['exchange'] = row['code'][:2]
        row['code'] = row['code'][3:]
        row['outDate'] = '"' + row['outDate'] + '"' if len(row['outDate']) > 0 else "NULL"        
        mdb.execute(conn, sql.format(**row))
    
    mdb.close(conn)
    print(f"{len(data)} info persisted")

def getInfo(exchange, code):
    data = getInfoFromDb(exchange, code)
    if(len(data.index) == 0):
        data = getInfoFromBao(exchange, code)
        persistInfo(data)
    
    return getInfoFromDb(exchange, code)
        
def getIpoDate(exchange, code):
    data = getInfo(exchange, code)
    return data.loc[0, 'ipo_date']