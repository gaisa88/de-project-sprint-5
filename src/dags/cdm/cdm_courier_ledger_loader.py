import logging
import pendulum
from airflow import DAG
from airflow.decorators import task
from lib import ConnectionBuilder
from config_const import ConfigConst

log = logging.getLogger(__name__)

with DAG(
    dag_id='cdm_courier_ledger_load',
    schedule_interval='0 2 1 * *',
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    catchup=False,
    tags=['cdm', 'courier_ledger'],
    is_paused_upon_creation=False
) as dag:

    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)

    @task(task_id="load_cdm_courier_ledger")
    def load_cdm_courier_ledger(prev_ds=None):
        with dwh_pg_connect.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    DELETE FROM cdm.dm_courier_ledger
                    WHERE settlement_year = EXTRACT(YEAR FROM DATE '{prev_ds}')
                      AND settlement_month = EXTRACT(MONTH FROM DATE '{prev_ds}');
                    WITH courier_month AS (
                        SELECT
                            f.courier_id,
                            c.name AS courier_name,
                            EXTRACT(YEAR FROM f.delivery_ts)::INT AS settlement_year,
                            EXTRACT(MONTH FROM f.delivery_ts)::INT AS settlement_month,
                            COUNT(f.id) AS orders_count,
                            SUM(f.sum) AS orders_total_sum,
                            ROUND(AVG(f.rate)::numeric, 2) AS rate_avg,
                            SUM(f.tip_sum) AS courier_tips_sum
                        FROM dds.fct_deliveries f
                        JOIN dds.dm_couriers c ON f.courier_id = c.id
                        WHERE EXTRACT(YEAR FROM f.delivery_ts) = EXTRACT(YEAR FROM DATE '{prev_ds}')
                          AND EXTRACT(MONTH FROM f.delivery_ts) = EXTRACT(MONTH FROM DATE '{prev_ds}')
                        GROUP BY f.courier_id, c.name, settlement_year, settlement_month
                    )
                    INSERT INTO cdm.dm_courier_ledger (
                        courier_id,
                        courier_name,
                        settlement_year,
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        order_processing_fee,
                        courier_order_sum,
                        courier_tips_sum,
                        courier_reward_sum
                    )
                    SELECT
                        courier_id,
                        courier_name,
                        settlement_year,
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        ROUND(orders_total_sum * 0.25, 2) AS order_processing_fee,
                        ROUND(
                            CASE
                                WHEN rate_avg < 4 THEN GREATEST(orders_total_sum * 0.05, 100)
                                WHEN rate_avg >= 4 AND rate_avg < 4.5 THEN GREATEST(orders_total_sum * 0.07, 150)
                                WHEN rate_avg >= 4.5 AND rate_avg < 4.9 THEN GREATEST(orders_total_sum * 0.08, 175)
                                ELSE GREATEST(orders_total_sum * 0.10, 200)
                            END
                        , 2) AS courier_order_sum,
                        courier_tips_sum,
                        ROUND(
                            CASE
                                WHEN rate_avg < 4 THEN GREATEST(orders_total_sum * 0.05, 100)
                                WHEN rate_avg >= 4 AND rate_avg < 4.5 THEN GREATEST(orders_total_sum * 0.07, 150)
                                WHEN rate_avg >= 4.5 AND rate_avg < 4.9 THEN GREATEST(orders_total_sum * 0.08, 175)
                                ELSE GREATEST(orders_total_sum * 0.10, 200)
                            END
                            + courier_tips_sum * 0.95, 2
                        ) AS courier_reward_sum
                    FROM courier_month;
                """)
            conn.commit()

        log.info(f"Загрузка витрины dm_courier_ledger завершена за месяц {prev_ds}")

    load_cdm_courier_ledger.override(
        templates_dict={"prev_ds": "{{ prev_ds }}"}
    )()
