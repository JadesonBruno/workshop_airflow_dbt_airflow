from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook  # ✅ CORRIGIDO
from pendulum import datetime

def test_psycopg2_connection(**context):
    """Teste direto com PostgresHook (mais confiável para Redshift)"""
    try:
        hook = PostgresHook(postgres_conn_id='aws_redshift_dw')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute('SELECT version();')
        version = cursor.fetchone()
        print(f"✅ SUCESSO: Conectado ao Redshift!")
        print(f"   Versão: {version[0]}")
        
        cursor.close()
        conn.close()
        return "Connection successful"
    except Exception as e:
        print(f"❌ ERRO: {str(e)}")
        raise

def test_sql_execution(**context):
    """Teste executando queries"""
    try:
        hook = PostgresHook(postgres_conn_id='aws_redshift_dw')
        
        # Query 1: Versão
        result = hook.get_first('SELECT version();')
        print(f"✅ Versão do Redshift: {result[0]}")
        
        # Query 2: Current user
        result = hook.get_first('SELECT current_user;')
        print(f"✅ Usuário: {result[0]}")
        
        # Query 3: Database
        result = hook.get_first('SELECT current_database();')
        print(f"✅ Database: {result[0]}")
        
        return "SQL execution successful"
    except Exception as e:
        print(f"❌ ERRO: {str(e)}")
        raise

with DAG(
    dag_id='test_redshift_connection',
    start_date=datetime(2025, 11, 4),
    schedule=None,
    catchup=False
) as dag:
    
    # Teste 1: Conectividade de rede (ping)
    test_ping = BashOperator(
        task_id='test_ping',
        bash_command='echo "🔍 Testando ping..." && ping -c 2 jornada-dw.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com || echo "⚠️  Ping não responde (esperado para Redshift)"'
    )
    
    # Teste 2: Porta aberta (telnet)
    test_port = BashOperator(
        task_id='test_port',
        bash_command='echo "🔍 Testando porta 5439..." && timeout 5 bash -c "cat < /dev/null > /dev/tcp/jornada-dw.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com/5439" && echo "✅ Porta 5439 está aberta" || echo "❌ Porta 5439 está fechada"'
    )
    
    # Teste 3: Conectividade com PostgresHook
    test_psycopg2 = PythonOperator(
        task_id='test_psycopg2_connection',
        python_callable=test_psycopg2_connection,
        doc="Testa conexão com Redshift usando PostgresHook"
    )
    
    # Teste 4: Executar queries
    test_sql_exec = PythonOperator(
        task_id='test_sql_execution',
        python_callable=test_sql_execution,
        doc="Testa execução de queries no Redshift"
    )
    
    test_ping >> test_port >> test_psycopg2 >> test_sql_exec
