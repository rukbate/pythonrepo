import mariadb
import sys

try:
    conn = mariadb.connect(
        user="ashare",
        password="synology@mariaDB10",
        host="192.168.11.11",
        port=3307,
        database="ashare"
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB: {e}")
    sys.exit(1)

cur = conn.cursor()
# cur.execute("insert into stock(code, name) values('000004', 'stock 4')")
# conn.commit()

cur.execute("select * from stock")

for(code, name) in cur:
    print(f"Code: {code}, Name: {name}")