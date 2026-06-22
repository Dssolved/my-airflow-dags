import pendulum

from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="test_db_connections",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["test", "connections"],
)
def test_db_connections():

    @task
    def check_mysql() -> None:
        hook = MySqlHook(mysql_conn_id="shop_mysql")

        result = hook.get_first("""
                                SELECT COUNT(*)
                                FROM users
                                """)

        print(f"MySQL connection successful. Users count: {result[0]}")

    @task
    def check_postgres() -> None:
        hook = PostgresHook(postgres_conn_id="analytics_postgres")

        result = hook.get_first("""
                                SELECT COUNT(*)
                                FROM information_schema.tables
                                WHERE table_schema = 'raw'
                                """)

        print(f"PostgreSQL connection successful. RAW tables count: {result[0]}")

    check_mysql()
    check_postgres()


test_db_connections()