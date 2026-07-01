import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


TABLE_CONFIG = {
    "wms_carriers": {
        "source_table": "carriers",
        "source_sql": """
                      SELECT id, name, service_level, base_cost, per_kg_cost, is_active
                      FROM carriers
                      ORDER BY id
                      """,
        "target_columns": ["id", "name", "service_level", "base_cost", "per_kg_cost", "is_active"],
    },
    "wms_warehouses": {
        "source_table": "warehouses",
        "source_sql": """
                      SELECT id, code, name, address, city, country_iso, lat, lon,
                             capacity_units, is_active, created_at
                      FROM warehouses
                      ORDER BY id
                      """,
        "target_columns": ["id", "code", "name", "address", "city", "country_iso", "lat", "lon",
                           "capacity_units", "is_active", "created_at"],
    },
    "wms_shipments": {
        "source_table": "shipments",
        "source_sql": """
                      SELECT id, order_id, warehouse_id, carrier_id, tracking_number,
                             status, weight_kg, total_cost, destination_city,
                             destination_country, created_at, picked_at, packed_at,
                             shipped_at, delivered_at
                      FROM shipments
                      ORDER BY id
                      """,
        "target_columns": ["id", "order_id", "warehouse_id", "carrier_id", "tracking_number",
                           "status", "weight_kg", "total_cost", "destination_city",
                           "destination_country", "created_at", "picked_at", "packed_at",
                           "shipped_at", "delivered_at"],
    },
    "wms_inventory": {
        "source_table": "inventory",
        "source_sql": """
                      SELECT id, warehouse_id, product_id, quantity_on_hand, reserved_qty,
                             reorder_threshold, updated_at
                      FROM inventory
                      ORDER BY id
                      """,
        "target_columns": ["id", "warehouse_id", "product_id", "quantity_on_hand", "reserved_qty",
                           "reorder_threshold", "updated_at"],
    },
    "wms_returns": {
        "source_table": "returns",
        "source_sql": """
                      SELECT id, shipment_id, reason, refund_amount, status, returned_at, processed_at
                      FROM returns
                      ORDER BY id
                      """,
        "target_columns": ["id", "shipment_id", "reason", "refund_amount", "status",
                           "returned_at", "processed_at"],
    },
}


@dag(
    dag_id="wms_full_load",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["wms", "postgres", "full-load"],
)
def wms_full_load():

    @task
    def load_table(table_name: str) -> int:
        config = TABLE_CONFIG[table_name]

        wms_hook = PostgresHook(postgres_conn_id="wms_postgres")
        analytics_hook = PostgresHook(postgres_conn_id="analytics_postgres")

        source_connection = wms_hook.get_conn()
        target_connection = analytics_hook.get_conn()

        source_cursor = source_connection.cursor()
        target_cursor = target_connection.cursor()

        try:
            source_cursor.execute(config["source_sql"])
            rows = source_cursor.fetchall()

            target_cursor.execute(f"TRUNCATE TABLE raw.{table_name}")

            columns = ", ".join(config["target_columns"])
            placeholders = ", ".join(["%s"] * len(config["target_columns"]))

            insert_sql = f"""
                INSERT INTO raw.{table_name} ({columns})
                VALUES ({placeholders})
            """

            if rows:
                target_cursor.executemany(insert_sql, rows)

            target_connection.commit()

            print(f"Loaded {len(rows)} rows into raw.{table_name}")

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
        wms_hook = PostgresHook(postgres_conn_id="wms_postgres")
        analytics_hook = PostgresHook(postgres_conn_id="analytics_postgres")

        errors = []

        for table_name, config in TABLE_CONFIG.items():
            source_result = wms_hook.get_first(
                f"SELECT COUNT(*) FROM {config['source_table']}"
            )
            target_result = analytics_hook.get_first(
                f"SELECT COUNT(*) FROM raw.{table_name}"
            )

            source_count = source_result[0]
            target_count = target_result[0]

            print(f"{table_name}: source={source_count}, target={target_count}")

            if source_count != target_count:
                errors.append(
                    f"{table_name}: source={source_count}, target={target_count}"
                )

        if errors:
            raise ValueError("Row count validation failed:\n" + "\n".join(errors))

        print("All WMS source and target row counts match")

    load_tasks = []

    for table_name in TABLE_CONFIG:
        task_instance = load_table.override(
            task_id=f"load_{table_name}"
        )(table_name)

        load_tasks.append(task_instance)

    validation = validate_counts()

    for load_task in load_tasks:
        load_task >> validation


wms_full_load()