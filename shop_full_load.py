import pendulum

from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


TABLE_CONFIG = {
    "countries": {
        "source_sql": """
                      SELECT
                          id,
                          iso_code,
                          name,
                          lat,
                          lon
                      FROM countries
                      ORDER BY id
                      """,
        "target_columns": [
            "id",
            "iso_code",
            "name",
            "lat",
            "lon",
        ],
    },
    "cities": {
        "source_sql": """
                      SELECT
                          id,
                          name,
                          country_iso,
                          population,
                          lat,
                          lon
                      FROM cities
                      ORDER BY id
                      """,
        "target_columns": [
            "id",
            "name",
            "country_iso",
            "population",
            "lat",
            "lon",
        ],
    },
    "users": {
        "source_sql": """
                      SELECT
                          id,
                          email,
                          full_name,
                          registered_at,
                          last_login_at,
                          signup_lat,
                          signup_lon
                      FROM users
                      ORDER BY id
                      """,
        "target_columns": [
            "id",
            "email",
            "full_name",
            "registered_at",
            "last_login_at",
            "signup_lat",
            "signup_lon",
        ],
    },
    "products": {
        "source_sql": """
                      SELECT
                          id,
                          title,
                          category,
                          subcategory,
                          brand,
                          color,
                          sizes,
                          material,
                          price,
                          stock,
                          image_url,
                          description,
                          created_at
                      FROM products
                      ORDER BY id
                      """,
        "target_columns": [
            "id",
            "title",
            "category",
            "subcategory",
            "brand",
            "color",
            "sizes",
            "material",
            "price",
            "stock",
            "image_url",
            "description",
            "created_at",
        ],
    },
    "orders": {
        "source_sql": """
                      SELECT
                          id,
                          user_id,
                          status,
                          total_amount,
                          address,
                          city_id,
                          country_iso,
                          order_lat,
                          order_lon,
                          created_at
                      FROM orders
                      ORDER BY id
                      """,
        "target_columns": [
            "id",
            "user_id",
            "status",
            "total_amount",
            "address",
            "city_id",
            "country_iso",
            "order_lat",
            "order_lon",
            "created_at",
        ],
    },
    "order_items": {
        "source_sql": """
                      SELECT
                          id,
                          order_id,
                          product_id,
                          quantity,
                          unit_price
                      FROM order_items
                      ORDER BY id
                      """,
        "target_columns": [
            "id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
        ],
    },
}


@dag(
    dag_id="shop_full_load",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["shop", "mysql", "postgres", "full-load"],
)
def shop_full_load():

    @task
    def load_table(table_name: str) -> int:
        config = TABLE_CONFIG[table_name]

        mysql_hook = MySqlHook(mysql_conn_id="shop_mysql")
        postgres_hook = PostgresHook(
            postgres_conn_id="analytics_postgres"
        )

        source_connection = mysql_hook.get_conn()
        target_connection = postgres_hook.get_conn()

        source_cursor = source_connection.cursor()
        target_cursor = target_connection.cursor()

        try:
            source_cursor.execute(config["source_sql"])
            rows = source_cursor.fetchall()

            target_cursor.execute(
                f"TRUNCATE TABLE raw.{table_name}"
            )

            columns = ", ".join(config["target_columns"])
            placeholders = ", ".join(
                ["%s"] * len(config["target_columns"])
            )

            insert_sql = f"""
                INSERT INTO raw.{table_name} ({columns})
                VALUES ({placeholders})
            """

            if rows:
                target_cursor.executemany(insert_sql, rows)

            target_connection.commit()

            print(
                f"Loaded {len(rows)} rows into "
                f"raw.{table_name}"
            )

            return len(rows)

        except Exception:
            target_connection.rollback()
            raise

        finally:
            source_cursor.close()
            target_cursor.close()
            source_connection.close()
            target_connection.close()

    @task
    def validate_counts() -> None:
        mysql_hook = MySqlHook(mysql_conn_id="shop_mysql")
        postgres_hook = PostgresHook(
            postgres_conn_id="analytics_postgres"
        )

        errors = []

        for table_name in TABLE_CONFIG:
            source_result = mysql_hook.get_first(
                f"SELECT COUNT(*) FROM {table_name}"
            )

            target_result = postgres_hook.get_first(
                f"SELECT COUNT(*) FROM raw.{table_name}"
            )

            source_count = source_result[0]
            target_count = target_result[0]

            print(
                f"{table_name}: "
                f"source={source_count}, "
                f"target={target_count}"
            )

            if source_count != target_count:
                errors.append(
                    f"{table_name}: "
                    f"source={source_count}, "
                    f"target={target_count}"
                )

        if errors:
            raise ValueError(
                "Row count validation failed:\n"
                + "\n".join(errors)
            )

        print("All source and target row counts match")

    load_tasks = []

    for table_name in TABLE_CONFIG:
        task_instance = load_table.override(
            task_id=f"load_{table_name}"
        )(table_name)

        load_tasks.append(task_instance)

    validation = validate_counts()

    for load_task in load_tasks:
        load_task >> validation


shop_full_load()