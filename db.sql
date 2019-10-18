create table users
(
    id char(42) not null primary key, -- address
    traffic bigint not null default 0,
);

create table buckets
(
    id         char(42) not null primary key,  -- bucketId
    size       bigint   not null default 0,
    time_start integer  not null,
    time_end   integer  not null,
    backup     integer  not null,
    name       text,
    uid        char(42) not null
);

create table settings
(
    id serial,
    head_block bigint not null default 0,
);
