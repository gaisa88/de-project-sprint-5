drop table stg.bonussystem_users 

-- stg.bonussystem_events definition

-- Drop table

-- DROP TABLE stg.bonussystem_events;
CREATE TABLE stg.bonussystem_users (
	id int4 NOT NULL,
	order_user_id text NOT NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id)
);

-- ==============================
-- Таблица restaurants
-- ==============================
create table if not exists stg.ordersystem_restaurants (
    object_id      varchar            not null primary key,  -- _id из MongoDB
    name           varchar,
    address        varchar,
    update_ts      timestamp not null                       -- когда запись обновлена
);

-- ==============================
-- Таблица users
-- ==============================
create table if not exists stg.ordersystem_users (
    object_id      varchar            not null primary key,  -- _id из MongoDB
    name           varchar,
    login          varchar,
    update_ts      timestamp not null
);

-- ==============================
-- Таблица orders
-- ==============================
create table if not exists stg.ordersystem_orders (
    object_id      varchar            not null primary key,  -- _id из MongoDB
    restaurant_id  varchar,                                  -- ссылка на ресторан
    user_id        varchar,                                  -- ссылка на пользователя
    order_ts       timestamp,                                -- когда заказ сделан
    order_sum      numeric(14,2),                            -- сумма заказа
    bonus_payment  numeric(14,2),                            -- сколько бонусами
    payment        numeric(14,2),                            -- сколько деньгами
    status         varchar,                                  -- статус заказа
    update_ts      timestamp not null
);


CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.srv_wf_settings (
    id SERIAL PRIMARY KEY,              -- суррогатный ключ
    workflow_key VARCHAR NOT NULL UNIQUE, -- ключ задачи (например, 'dds_load_orders')
    workflow_settings JSONB NOT NULL,     -- JSON с параметрами (курсор, таймстемпы и т.п.)
    updated_at TIMESTAMP DEFAULT NOW()    -- техническое поле, когда последний раз обновлялось
);
ALTER TABLE dds.dm_timestamps
    ADD CONSTRAINT dm_timestamps_ts_key UNIQUE (ts);

CREATE TABLE cdm.dm_settlement_report (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    restaurant_id int NOT NULL,
    restaurant_name VARCHAR NOT NULL,
    settlement_date DATE NOT NULL CHECK(settlement_date > '2022-01-01' AND settlement_date < '2050-01-01'),
    orders_count int NOT NULL CHECK(orders_count >= 0),
    orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0),
    orders_bonus_payment_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_bonus_payment_sum >= 0),
    orders_bonus_granted_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_bonus_granted_sum >= 0),
    order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0),
    restaurant_reward_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (restaurant_reward_sum >= 0),
    UNIQUE(restaurant_id, settlement_date)
);

;
truncate table cdm.dm_settlement_report 

WITH order_sums AS (
    SELECT
        r.id                    AS restaurant_id,
        r.restaurant_name       AS restaurant_name,
        tss.date                AS settlement_date,
        COUNT(DISTINCT o.id)    AS orders_count,                     -- <-- исправлено
        SUM(f.total_sum)        AS orders_total_sum,
        SUM(f.bonus_payment)    AS orders_bonus_payment_sum,
        SUM(f.bonus_grant)      AS orders_bonus_granted_sum
    FROM dds.fct_product_sales AS f
    JOIN dds.dm_orders          AS o  ON f.order_id = o.id
    JOIN dds.dm_timestamps     AS tss ON o.timestamp_id = tss.id
    JOIN dds.dm_restaurants    AS r  ON o.restaurant_id = r.id
    -- учти регистр статуса: привожу к верхнему, чтобы совпадало и 'closed' и 'CLOSED'
    WHERE UPPER(o.order_status) = 'CLOSED'
    GROUP BY
        r.id,
        r.restaurant_name,
        tss.date
)
INSERT INTO cdm.dm_settlement_report(
    restaurant_id,
    restaurant_name,
    settlement_date,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    order_processing_fee,
    restaurant_reward_sum
)
SELECT
    restaurant_id,
    restaurant_name,
    settlement_date,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    orders_total_sum * 0.25 AS order_processing_fee,
    orders_total_sum - orders_total_sum * 0.25 - orders_bonus_payment_sum AS restaurant_reward_sum
FROM order_sums
ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
SET
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;

