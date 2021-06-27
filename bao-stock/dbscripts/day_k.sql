create table day_k (
    exchange	    char(2),
    date		    date,
    code 		    char(6),
    open		    float(10,2),
    high		    float(10,2),
    low			    float(10,2),
    close		    float(10,2),
    pre_close	    float(10,2),
    volume		    bigint(20),
    amount		    double(20,2),
    adjust_flag	    int(1),
    turn		    float(12,6),
    trade_status	int(1),
    pct_chg		    float(12,6),
    pe_ttm		    float(12,6),
    pb_mrq		    float(12,6),
    ps_ttm		    float(12,6),
    pcf_nc_ttm	    float(12,6),
    is_st		    int(1),
    primary key (code, date)
);

create index day_k_idx1 on day_k(exchange, is_st);