from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Een simpele Python functie die we gaan aanroepen vanuit de DAG
def print_hello():
    print("Hallo vanuit Airflow!")
    return "Taak succesvol afgerond!"

# Default instellingen voor de DAG
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definieer de DAG
with DAG(
    dag_id='mijn_eerste_basic_dag',
    default_args=default_args,
    description='Een hele simpele test DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:

    # Taak 1: Een simpele bash command
    taak_1 = BashOperator(
        task_id='zeg_hallo_in_bash',
        bash_command='echo "Dit is stap 1: Data ophalen (fictief)"',
    )

    # Taak 2: Een Python functie
    taak_2 = PythonOperator(
        task_id='voer_python_script_uit',
        python_callable=print_hello,
    )

    # Taak 3: Nog een bash command
    taak_3 = BashOperator(
        task_id='klaar',
        bash_command='echo "Alles is succesvol gedraaid!"',
    )

    # Definieer de volgorde van de taken (dependencies)
    taak_1 >> taak_2 >> taak_3
