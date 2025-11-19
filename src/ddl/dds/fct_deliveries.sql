CREATE TABLE dds.fct_deliveries (
	id serial4 NOT NULL,
	order_id int4 NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_id varchar(50) NOT NULL,
	courier_id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	address text NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate int4 NULL,
	sum numeric(12, 2) NULL,
	tip_sum numeric(12, 2) NULL,
	CONSTRAINT fct_deliveries_delivery_id_key UNIQUE (delivery_id),
	CONSTRAINT fct_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT fct_deliveries_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id),
	CONSTRAINT fct_deliveries_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
	CONSTRAINT fct_deliveries_restaurant_sk_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id)
);