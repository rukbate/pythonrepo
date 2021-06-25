import mariadb
import sys

def dbConnect(host, port, user, password, database):
    try:
        conn = mariadb.connect(
            host = host,
            port = port,
            user = user,
            password = password,
            database = database
        )
        return conn
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB: {e}")
        sys.exit(1)

def dbQuery(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    return cur

def dbExecute(conn, sql):
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except mariadb.Error as err:
        print(f"Error execute sql: {err}\n\t {sql}")

def dbClose(conn):
    conn.close()