create table if not exists stg.api_couriers (
    id serial primary key,
    courier_id varchar(50) not null unique,
    name varchar(200) not null
);
