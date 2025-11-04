from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pendulum import datetime

with DAG(
    dag_id='test_redshift_connection',
    start_date=datetime(2025, 11, 4),
    schedule=None,
    catchup=False
) as dag:
    
    # Testar conectividade com ping
    test_ping = BashOperator(
        task_id='test_ping',
        bash_command='ping -c 3 jornada-dw.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com || echo "Ping failed (expected for Redshift)"'
    )
    
    # Testar conectividade com telnet
    test_port = BashOperator(
        task_id='test_port',
        bash_command='timeout 5 bash -c "cat < /dev/null > /dev/tcp/jornada-dw.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com/5439" && echo "Port 5439 is open" || echo "Port 5439 is closed"'
    )
    
    # Testar query SQL
    test_query = PostgresOperator(
        task_id='test_query',
        postgres_conn_id='aws_redshift_dw',
        sql='SELECT 1 as test;'
    )
    
    test_ping >> test_port >> test_query
