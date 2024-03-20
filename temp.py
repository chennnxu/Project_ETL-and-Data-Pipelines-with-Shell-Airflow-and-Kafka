# Import the libraries

from datetime import timedelta
# The DAG object to instantiate a DAG
from airflow import DAG 
# Operators used to write tasks
from airflow.operators.bash_operator import Bash_Operator
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
    bash_command = 'cut -d -f1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > \
        /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag = dag,
)


# define a task to extact data from tsv file

extract_data_from_tsv = Bash_Operator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'cut -d -f5-7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > \
        /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag = dag,
)

# define a task to extract data from fixed width txt file

extract_data_from_fixed_width = Bash_Operator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = 'cut -c59-67 /home/project/airflow/dags/finalassignment/payment-data.txt > \
        /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
    dag = dag,
)

# define a task to consolidate data extracted from previous tasks

consolidate_data = Bash_Operator(
    task_id = 'consolidate_data',
    bash_command = 'paste /home/project/airflow/dags/finalassignment/csv_data.csv \
        /home/project/airflow/dags/finalassignment/tsv_data.csv \
        /home/project/airflow/dags/finalassignment/fixed_width_data.csv \
        > /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag = dag,
)

# Transform and load data

transform_data = Bash_Operator(
    task_id = 'transform_data',
    bash_command = 'tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/finalassignment/extracted_data.csv \
        > /home/project/airflow/dags/finalassignment/transformed_data.csv',
    dag = dag,
)

# Define the task pipeline

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width \
    >> consolidate_data >> transform_data