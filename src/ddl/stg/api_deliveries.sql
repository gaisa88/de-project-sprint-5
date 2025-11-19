create table if not exists stg.api_deliveries (
    id serial primary key,
    order_id varchar(50) not null unique,
    order_ts timestamp not null,
    delivery_id varchar(50) not null,
    courier_id varchar(50) not null references stg.api_couriers(courier_id),
    address text not null,
    delivery_ts timestamp not null,
    rate int,
    sum numeric(12,2),
    tip_sum numeric(12,2),
    restaurant_id varchar(50) REFERENCES stg.api_restaurants(restaurant_id)
);