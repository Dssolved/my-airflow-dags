from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'wms_full_load',
    default_args=default_args,
    description='Full load WMS PostgreSQL -> Analytics PostgreSQL RAW layer',
    schedule_interval=None,
    catchup=False,
    tags=['wms', 'raw'],
)

def load_table(wms_conn_id, analytics_conn_id, table_name, query, insert_sql, **kwargs):
    wms_hook = PostgresHook(postgres_conn_id=wms_conn_id)
    analytics_hook = PostgresHook(postgres_conn_id=analytics_conn_id)

    logging.info(f'Extracting {table_name} from WMS...')
    rows = wms_hook.get_records(query)
    logging.info(f'Extracted {len(rows)} rows from {table_name}')

    analytics_conn = analytics_hook.get_conn()
    analytics_cursor = analytics_conn.cursor()

    try:
        analytics_cursor.execute(f'TRUNCATE TABLE raw.{table_name}')
        analytics_cursor.executemany(insert_sql, rows)
        analytics_conn.commit()
        logging.info(f'Loaded {len(rows)} rows into raw.{table_name}')
    except Exception as e:
        analytics_conn.rollback()
        logging.error(f'Error loading {table_name}: {e}')
        raise
    finally:
        analytics_cursor.close()
        analytics_conn.close()


def validate_counts(**kwargs):
    wms_hook = PostgresHook(postgres_conn_id='wms_postgres')
    analytics_hook = PostgresHook(postgres_conn_id='analytics_postgres')

    tables = ['carriers', 'warehouses', 'shipments', 'inventory', 'returns']

    for table in tables:
        wms_count = wms_hook.get_first(f'SELECT COUNT(*) FROM {table}')[0]
        raw_count = analytics_hook.get_first(f'SELECT COUNT(*) FROM raw.wms_{table}')[0]
        logging.info(f'{table}: WMS={wms_count}, RAW={raw_count}')
        if wms_count != raw_count:
            raise ValueError(f'Count mismatch for {table}: WMS={wms_count}, RAW={raw_count}')

    logging.info('All WMS counts validated successfully')


from airflow.operators.python import PythonOperator

load_carriers = PythonOperator(
    task_id='load_carriers',
    python_callable=load_table,
    op_kwargs={
        'wms_conn_id': 'wms_postgres',
        'analytics_conn_id': 'analytics_postgres',
        'table_name': 'wms_carriers',
        'query': 'SELECT id, name, service_level, base_cost, per_kg_cost, is_active FROM carriers',
        'insert_sql': 'INSERT INTO raw.wms_carriers (id, name, service_level, base_cost, per_kg_cost, is_active) VALUES (%s, %s, %s, %s, %s, %s)',
    },
    dag=dag,
)

load_warehouses = PythonOperator(
    task_id='load_warehouses',
    python_callable=load_table,
    op_kwargs={
        'wms_conn_id': 'wms_postgres',
        'analytics_conn_id': 'analytics_postgres',
        'table_name': 'wms_warehouses',
        'query': 'SELECT id, code, name, address, city, country_iso, lat, lon, capacity_units, is_active, created_at FROM warehouses',
        'insert_sql': 'INSERT INTO raw.wms_warehouses (id, code, name, address, city, country_iso, lat, lon, capacity_units, is_active, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
    },
    dag=dag,
)

load_shipments = PythonOperator(
    task_id='load_shipments',
    python_callable=load_table,
    op_kwargs={
        'wms_conn_id': 'wms_postgres',
        'analytics_conn_id': 'analytics_postgres',
        'table_name': 'wms_shipments',
        'query': '''
                 SELECT id, order_id, warehouse_id, carrier_id, tracking_number,
                        status, weight_kg, total_cost, destination_city,
                        destination_country, created_at, picked_at, packed_at,
                        shipped_at, delivered_at
                 FROM shipments
                 ''',
        'insert_sql': '''
                      INSERT INTO raw.wms_shipments (
                          id, order_id, warehouse_id, carrier_id, tracking_number,
                          status, weight_kg, total_cost, destination_city,
                          destination_country, created_at, picked_at, packed_at,
                          shipped_at, delivered_at
                      ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                      ''',
    },
    dag=dag,
)

load_inventory = PythonOperator(
    task_id='load_inventory',
    python_callable=load_table,
    op_kwargs={
        'wms_conn_id': 'wms_postgres',
        'analytics_conn_id': 'analytics_postgres',
        'table_name': 'wms_inventory',
        'query': 'SELECT id, warehouse_id, product_id, quantity_on_hand, reserved_qty, reorder_threshold, updated_at FROM inventory',
        'insert_sql': 'INSERT INTO raw.wms_inventory (id, warehouse_id, product_id, quantity_on_hand, reserved_qty, reorder_threshold, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s)',
    },
    dag=dag,
)

load_returns = PythonOperator(
    task_id='load_returns',
    python_callable=load_table,
    op_kwargs={
        'wms_conn_id': 'wms_postgres',
        'analytics_conn_id': 'analytics_postgres',
        'table_name': 'wms_returns',
        'query': 'SELECT id, shipment_id, reason, refund_amount, status, returned_at, processed_at FROM returns',
        'insert_sql': 'INSERT INTO raw.wms_returns (id, shipment_id, reason, refund_amount, status, returned_at, processed_at) VALUES (%s, %s, %s, %s, %s, %s, %s)',
    },
    dag=dag,
)

validate = PythonOperator(
    task_id='validate_counts',
    python_callable=validate_counts,
    dag=dag,
)

# Порядок выполнения
load_carriers >> load_warehouses >> load_shipments >> load_inventory >> load_returns >> validate