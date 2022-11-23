create table profit (
    code            char(9),
    pub_date        date,
    stat_date       date,
    roe_avg         float(10, 2),
    np_margin       float(10, 2),
    gp_margin       float(10, 2),
    net_profit      float(20, 2),
    eps_ttm         float(10, 2),
    mb_revenue      float(20, 2),
    total_share     float(20, 2),
    liqa_share      float(20, 2),
    primary key (code, pub_date, stat_date)
);