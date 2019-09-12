create table users
(
    address char(42) not null primary key,
    traffic bigint not null default 0,
);

create table buckets
(
    id         char(42) not null primary key,
    size       bigint   not null default 0,
    time_start integer  not null,
    time_end   integer  not null,
    backup     integer  not null,
    name       text,
    uid        char(42) not null
);
