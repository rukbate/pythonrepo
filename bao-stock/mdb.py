import mariadb
import sys
import numpy as np
import pandas as pd
import configparser

def connectAShare():
    cp = configparser.ConfigParser()
    cp.read('dbscripts/db.properties')
    config = cp['DEFAULT']
    return connect(
        config['dbServer'], 
        config.getint('dbPort'), 
        config['username'],
        config['password'], 
        config['database']
    )

def connect(host, port, user, password, database):
    try:
        return mariadb.connect(
            host = host,
            port = port,
            user = user,
            password = password,
            database = database
        )
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

def executeMany(conn, sql, data):
    try:
        cur = conn.cursor()
        cur.executemany(sql, data)
        conn.commit()
    except mariadb.Error as err:
        print(f"Error execute sql: {err}")

def close(conn):
    conn.close()