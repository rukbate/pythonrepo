import bao
import mdb

indexes = {
    'sz50': bao.querySZ50Stocks,
    'hs300': bao.queryHS300Stocks,
    'zz500': bao.queryZZ500Stocks
}

def deleteIndex(type):
    sql = f"delete from idx where type = '{type}'"

    conn = mdb.connectAShare()
    mdb.execute(conn, sql)
    print(f"{type} deleted")
    mdb.close(conn)

def persistIndex(type, data):
    sql = "insert into idx(type, exchange, code, name, update_date) values(?, ?, ?, ?, ?)"
    indexes = []
    
    for index, row in data.iterrows():
        indexes.append((type, row['code'][:2], row['code'][3:], row['code_name'], row['updateDate']))
    
    conn = mdb.connectAShare()
    mdb.executeMany(conn, sql, indexes)
    mdb.close(conn)
    print(f"{len(data)} {type} persisted")

def updateIndex(type):
    if not type in indexes:
        print(f"Invalid index type: {type}")
        return

    data = indexes[type]()
    if len(data.index) > 0:    
        deleteIndex(type)
        persistIndex(type, data)

def updateAllIndexes():
    for k in indexes:
        updateIndex(k)
