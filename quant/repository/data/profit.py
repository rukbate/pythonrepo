from repository import mdb
from util.stringutil import normalize_number as normalize
from repository.data import basic as bc
from datetime import date
from util import dateutil as du
from market import market
from datetime import timedelta


def persist_profit(data):
    sql = """insert into profit(code, pub_date, stat_date, roe_avg, np_margin, gp_margin, net_profit, eps_ttm, mb_revenue, total_share, liqa_share)
                 values('{code}', '{pubDate}', '{statDate}', {roeAvg}, {npMargin}, {gpMargin}, {netProfit}, {epsTTM}, {MBRevenue}, {totalShare}, {liqaShare})
              """

    conn = mdb.connect_db()
    for index, row in data.iterrows():
        row['code'] = row['code']
        row['roeAvg'] = normalize(row['roeAvg'])
        row['npMargin'] = normalize(row['npMargin'])
        row['gpMargin'] = normalize(row['gpMargin'])
        row['netProfit'] = normalize(row['netProfit'])
        row['epsTTM'] = normalize(row['epsTTM'])
        row['MBRevenue'] = normalize(row['MBRevenue'])
        row['totalShare'] = normalize(row['totalShare'])
        row['liqaShare'] = normalize(row['liqaShare'])
        mdb.execute(conn, sql.format(**row))

    mdb.close(conn)
    print(f"{len(data)} profit data persisted")


def retrieve_profit(code):
    conn = mdb.connect_db()
    data = mdb.query(
        conn,
        f"select * from profit where code = '{code}' order by stat_date desc"
    )

    mdb.close(conn)
    return data


def update_profit(code):
    print(f'Updating profit data for {code}')
    start_date = bc.ipo_date(code)
    db_profits = retrieve_profit(code)

    if not db_profits.empty:
        start_date = db_profits.to_numpy()[0][2]

    today = date.today()

    start_year = du.year(start_date)
    start_quarter = du.quarter(start_date)

    end_year = du.year(today)
    end_quarter = du.quarter(today)

    cur_year = start_year
    while cur_year <= end_year:
        for cur_quarter in [1, 2, 3, 4]:
            if cur_year == start_year and cur_quarter <= start_quarter:
                continue
            elif cur_year == end_year and cur_quarter >= end_quarter:
                continue
            else:
                persist_profit(market.profit(code, cur_year, cur_quarter))
        cur_year += 1


def update_all_profit():
    stocks = market.all_stocks((date.today() + timedelta(days=-1)).isoformat())

    for code, status, name in stocks.to_numpy():
        if code[:2] != 'bj' and code[:6] != 'sh.000':
            update_profit(code)
