from market import market
from repository import mdb


def db_info(exchange, code):
    sql = f"select * from info where exchange = '{exchange}' and code = '{code}'"
    conn = mdb.connect_db()
    data = mdb.query(conn, sql)

    mdb.close(conn)
    return data


def persist_info(data):
    sql = """insert into info(exchange, code, name, latest_date, out_date, type, status)
             values('{exchange}', '{code}', '{code_name}', '{ipoDate}', {outDate}, {type}, {status})
          """

    conn = mdb.connect_db()
    for index, row in data.iterrows():
        row['exchange'] = row['code'][:2]
        row['code'] = row['code'][3:]
        row['outDate'] = '"' + row['outDate'] + '"' if len(row['outDate']) > 0 else "NULL"
        mdb.execute(conn, sql.format(**row))

    mdb.close(conn)
    print(f"{len(data)} info persisted")


def retrieve_info(exchange, code):
    data = db_info(exchange, code)
    if len(data.index) == 0:
        data = market.stock_basic(exchange, code)
        persist_info(data)

    return db_info(exchange, code)


def ipo_date(exchange, code):
    data = retrieve_info(exchange, code)
    return data.loc[0, 'latest_date']
