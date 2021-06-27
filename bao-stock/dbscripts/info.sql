create table info (
    exchange        char(2),
    code            char(6),
    name            varchar(10),
    ipo_date        date,
    out_date        date,
    type            int(1),
    status          int(1),
    primary key (exchange, code)
);

create index info_idx1 on info(ipo_date, exchange);