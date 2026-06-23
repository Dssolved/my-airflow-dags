from pathlib import Path

import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


SQL_DIR = Path(__file__).parent / "sql" / "refresh"


def execute_sql_file(filename: str) -> None:
    sql_path = SQL_DIR / filename

    if not sql_path.exists():
        raise FileNotFoundError(
            f"SQL file not found: {sql_path}"
        )

    sql = sql_path.read_text(encoding="utf-8")

    postgres_hook = PostgresHook(
        postgres_conn_id="analytics_postgres"
    )

    postgres_hook.run(sql)

    print(f"Successfully executed: {filename}")


@dag(
    dag_id="shop_build_marts",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    tags=["shop", "postgres", "marts"],
)
def shop_build_marts():

    @task
    def refresh_orders() -> None:
        execute_sql_file(
            "refresh_mart_orders.sql"
        )

    @task
    def refresh_sales_items() -> None:
        execute_sql_file(
            "refresh_mart_sales_items.sql"
        )

    @task
    def refresh_rfm_customers() -> None:
        execute_sql_file(
            "refresh_mart_rfm_customers.sql"
        )

    @task
    def validate_marts() -> None:
        postgres_hook = PostgresHook(
            postgres_conn_id="analytics_postgres"
        )

        raw_orders = postgres_hook.get_first("""
                                             SELECT COUNT(*)
                                             FROM raw.orders
                                             """)[0]

        mart_orders = postgres_hook.get_first("""
                                              SELECT COUNT(*)
                                              FROM mart.orders
                                              """)[0]

        raw_items = postgres_hook.get_first("""
                                            SELECT COUNT(*)
                                            FROM raw.order_items
                                            """)[0]

        mart_items = postgres_hook.get_first("""
                                             SELECT COUNT(*)
                                             FROM mart.sales_items
                                             """)[0]

        source_rfm_customers = postgres_hook.get_first("""
                                                       SELECT COUNT(DISTINCT user_id)
                                                       FROM raw.orders
                                                       WHERE status = 'delivered'
                                                       """)[0]

        mart_rfm_customers = postgres_hook.get_first("""
                                                     SELECT COUNT(*)
                                                     FROM mart.rfm_customers
                                                     """)[0]

        raw_revenue = postgres_hook.get_first("""
                                              SELECT COALESCE(SUM(total_amount), 0)
                                              FROM raw.orders
                                              WHERE status = 'delivered'
                                              """)[0]

        rfm_revenue = postgres_hook.get_first("""
                                              SELECT COALESCE(SUM(monetary), 0)
                                              FROM mart.rfm_customers
                                              """)[0]

        checks = {
            "orders count": (
                raw_orders,
                mart_orders,
            ),
            "sales items count": (
                raw_items,
                mart_items,
            ),
            "RFM customers count": (
                source_rfm_customers,
                mart_rfm_customers,
            ),
            "delivered revenue": (
                raw_revenue,
                rfm_revenue,
            ),
        }

        errors = []

        for check_name, values in checks.items():
            expected, actual = values

            print(
                f"{check_name}: "
                f"expected={expected}, "
                f"actual={actual}"
            )

            if expected != actual:
                errors.append(
                    f"{check_name}: "
                    f"expected={expected}, "
                    f"actual={actual}"
                )

        if errors:
            raise ValueError(
                "Mart validation failed:\n"
                + "\n".join(errors)
            )

        print("All marts validated successfully")

    orders_task = refresh_orders()
    sales_items_task = refresh_sales_items()
    rfm_task = refresh_rfm_customers()

    validation_task = validate_marts()

    [
        orders_task,
        sales_items_task,
        rfm_task,
    ] >> validation_task


shop_build_marts()