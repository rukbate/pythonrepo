from market import market
from repository import mdb


def delete_index(idx):
    sql = f"delete from idx where type = '{idx}'"

    conn = mdb.connect_db()
    mdb.execute(conn, sql)
    print(f"{idx} deleted")
    mdb.close(conn)


def persist_index(idx, data):
    sql = "insert into idx(type, exchange, code, name, update_date) values(?, ?, ?, ?, ?)"
    indexes = [
        (
            idx,
            row['code'][:2],
            row['code'][3:],
            row['code_name'],
            row['updateDate'],
        )
        for index, row in data.iterrows()
    ]

    conn = mdb.connect_db()
    mdb.execute_batch(conn, sql, indexes)
    mdb.close(conn)
    print(f"{len(data)} {idx} persisted")


def update_index(idx):
    data = market.index_stocks(idx)
    if len(data.index) > 0:
        delete_index(idx)
        persist_index(idx, data)


def update_indexes():
    for k in ['sz50', 'hs300', 'zz500']:
        update_index(k)


def retrieve_ndex(idx):
    conn = mdb.connect_db()
    data = mdb.query(
        conn,
        f"select * from idx where type = '{idx}'"
    )

    mdb.close(conn)
    return data
