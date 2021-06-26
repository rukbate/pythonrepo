create table idx (
    type            varchar(10),
    market          char(2),
    code            char(6),
    name            varchar(10),
    update_date     date,
    primary key (type, market, code)
);