create table dailyk (
    code	        char(9),
    date		    date,
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
    pcf_nc_ttm	    float(20,6),
    is_st		    int(1),
    primary key (code, date)
)
partition by key(code)
partitions 8;

create index dailyk_idx1 on dailyk(code, is_st);
