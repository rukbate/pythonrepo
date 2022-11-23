create table idx (
    type            varchar(10),
    code            char(9),
    name            varchar(255),
    update_date     date,
    primary key (type, code)
);