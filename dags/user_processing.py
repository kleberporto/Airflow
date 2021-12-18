from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime
from pandas import json_normalize
import json

TEMP_USER_CSV_LOCATION = '/tmp/processed_user.csv'
SQLITE_DATABASE_LOCATION = '/home/airflow/airflow/airflow.db'

default_args = {
    'start_date': datetime(2020, 1, 1)
}


def _processing_user(task_instance):
    """Retrieves the data from the extracted user, select the useful information
     and stores it in a csv file.

    Args:
        task_instance (Airflow Task Instance Object): Extracts data from another
        airflow task stored in the Airflow DB

    Raises:
        ValueError: Raises ValueError if the user object extracted from the 
        task instance xcom is empty
    """
    users = task_instance.xcom_pull(task_ids=['extracting_user'])
    if not len(users) or 'results' not in users[0]:
        raise ValueError('User is Empty')

    user = users[0]['results'][0]

    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    })
    processed_user.to_csv(TEMP_USER_CSV_LOCATION, index=None, header=False)



with DAG(dag_id='user_processing', schedule_interval='@daily',
        default_args=default_args, catchup=True) as dag:

    creating_table = SqliteOperator(
        task_id='creating_table',
        sqlite_conn_id='db_sqlite', # Used in Airflow UI to create the connection
        sql='''
            CREATE TABLE IF NOT EXISTS users(
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL PRIMARY KEY
            );
            '''
    )
    
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )

    extracting_user = SimpleHttpOperator(
        task_id='extracting_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True  # Show the response of the URL directly in the log
    )

    processing_user = PythonOperator(
        task_id='processing_user',
        python_callable=_processing_user
    )

    # Imports the user in the SQLite Database
    storing_user = BashOperator(
        task_id='storing_user',
        bash_command=f'echo -e ".separator ","\n.import {TEMP_USER_CSV_LOCATION} users" | sqlite3 {SQLITE_DATABASE_LOCATION}'
    )

    # Defining DAG Dependencies
    creating_table >> is_api_available >> extracting_user >> processing_user >> storing_user