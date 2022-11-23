create table basic (
    code            char(9),
    name            varchar(255),
    ipo_date        date,
    out_date        date,
    type            int(1),
    status          int(1),
    primary key (code)
);

create index basic_idx1 on basic(ipo_date);