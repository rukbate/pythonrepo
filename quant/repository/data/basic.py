from market import market
from repository import mdb
from datetime import date


def db_basic(code):
    sql = f"select * from basic where code = '{code}'"
    conn = mdb.connect_db()
    data = mdb.query(conn, sql)

    mdb.close(conn)
    return data


def persist_basic(data):
    sql = """insert into basic(code, name, ipo_date, out_date, type, status)
             values('{code}', '{code_name}', '{ipoDate}', {outDate}, {type}, {status})
          """

    conn = mdb.connect_db()
    for index, row in data.iterrows():
        row['code'] = row['code']
        row['outDate'] = '"' + row['outDate'] + '"' if len(row['outDate']) > 0 else "NULL"
        mdb.execute(conn, sql.format(**row))

    mdb.close(conn)
    print(f"{len(data)} basic persisted")


def retrieve_basic(code):
    data = db_basic(code)
    if len(data.index) == 0:
        data = market.stock_basic(code)
        persist_basic(data)

    return db_basic(code)


def ipo_date(code):
    data = retrieve_basic(code)

    return data.loc[0, 'ipo_date'] if not data.empty else date(1990, 1, 1)
