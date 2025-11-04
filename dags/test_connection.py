from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
import socket

def test_connection_details(**context):
    """Testa detalhes da Connection"""
    try:
        from airflow.models import Connection
        from airflow import settings
        
        session = settings.Session()
        conn = session.query(Connection).filter(Connection.conn_id == 'aws_redshift_dw').first()
        
        if not conn:
            print("❌ Connection 'aws_redshift_dw' não encontrada!")
            return
        
        print(f"✅ Connection encontrada!")
        print(f"   Host: {conn.host}")
        print(f"   Port: {conn.port}")
        print(f"   Schema: {conn.schema}")
        print(f"   Login: {conn.login}")
        
        session.close()
        return "Connection found"
    except Exception as e:
        print(f"❌ ERRO ao obter Connection: {str(e)}")
        raise

def test_dns_resolution(**context):
    """Testa resolução de DNS"""
    try:
        host = "jornada-dw.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com"
        print(f"🔍 Tentando resolver DNS: {host}")
        
        ip = socket.gethostbyname(host)
        print(f"✅ DNS resolvido para: {ip}")
        return ip
    except socket.gaierror as e:
        print(f"❌ ERRO ao resolver DNS: {str(e)}")
        # Não vai levantar erro - apenas informativo
        return None
    except Exception as e:
        print(f"❌ ERRO: {str(e)}")
        raise

def test_psycopg2_connection(**context):
    """Teste direto com PostgresHook"""
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
        print(f"❌ ERRO ao conectar: {str(e)}")
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
        print(f"❌ ERRO ao executar queries: {str(e)}")
        raise

with DAG(
    dag_id='test_redshift_connection',
    start_date=datetime(2025, 11, 4),
    schedule=None,
    catchup=False
) as dag:
    
    # Teste 1: Detalhes da Connection
    test_conn_details = PythonOperator(
        task_id='test_connection_details',
        python_callable=test_connection_details,
        doc="Verifica detalhes da Connection armazenada"
    )
    
    # Teste 2: Resolução de DNS
    test_dns = PythonOperator(
        task_id='test_dns_resolution',
        python_callable=test_dns_resolution,
        doc="Testa se consegue resolver o DNS do Redshift"
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
    
    test_conn_details >> test_dns >> test_psycopg2 >> test_sql_exec