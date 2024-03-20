# Import the libraries

from datetime import timedelta
# The DAG object to instantiate a DAG
from airflow import DAG 
# Operators used to write tasks
from airflow.Operators.bash_operator import Bash_Operator
# utils make scheduling easy
from airflow.utils.dates import days_ago

# Define DAG arguments

default_args = {
    # Owner name
    'owner': 'Chen',
    # When this DAG should run from, days_ago(0) means today
    'start_date': days_ago(0),
    # The email where the alerts are sent to
    'email': 'xc.ecnu@gmail.com',
    # whether alert must be sent on failure
    'email_on_failure': True,
    # whether alert must be sent on retry
    'email_on_retry': True,
    # the number of reties in case of failure
    'retries': 1,
    # the time delay between retries
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG

dag = DAG(
    dag_id = 'ETL_toll_data',
    schedule_interval = timedelta(days=1),
    default_args = default_args,
    description = 'ETL DAG for road traffic',
)


# Define the tasks

# define a task to unzip data
unzip_data = Bash_Operator(
    task_id = 'unzip_data',
    bash_command = 'tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag=dag,
)

# define a task to extract data from csv file
extract_data_from_csv = Bash_Operator(
    task_id = 'extract_data_from_csv',
    bash_commandd = 'cut -dsou'
)

