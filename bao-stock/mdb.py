import mariadb
import sys
import numpy as np
import pandas as pd

dbServer = '192.168.11.11'
dbPort = 3307
username = 'ashare'
password = 'synology@mariaDB10'
database = 'ashare'

def connectAShare():
    return connect(dbServer, dbPort, username, password, database)

def connect(host, port, user, password, database):
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

def query(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    
    columns = np.asarray(cur.description)[:, 0]
    return pd.DataFrame(cur.fetchall(), columns=columns)

def execute(conn, sql):
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except mariadb.Error as err:
        print(f"Error execute sql: {err}")

def close(conn):
    conn.close()