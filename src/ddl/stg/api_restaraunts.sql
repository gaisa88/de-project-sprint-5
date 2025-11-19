create table if not exists stg.api_restaurants (
    id serial primary key,
    restaurant_id varchar(50) not null unique,
    name varchar(200) not null
);