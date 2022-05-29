# Import the standard Airflow libraries
from airflow.models import DAG
from datetime import datetime, date, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.contrib.sensors.file_sensor import FileSensor
import pendulum

# Import the python scripts as classes and define methods out of these classes
def home_page_spider():
    from homzmart_scraping.homzmart_scraping.spiders.homzmart_home_page_spider import HomePageSpider

def cat_page_spider():
    from homzmart_scraping.homzmart_scraping.spiders.homzmart_cat_page_spider import CatPageSpider

def subcat_page_spider():
    from homzmart_scraping.homzmart_scraping.spiders.homzmart_subcat_page_spider import SubCatPageSpider

def prod_page_spider():
    from homzmart_scraping.homzmart_scraping.spiders.homzmart_prod_page_spider import ProdPageSpider

def combine_jsons():
    from homzmart_scraping.homzmart_scraping.spiders.homzmart_combine_jsons import CombineJsons

def delete_tables():
    from homzmart_scraping.homzmart_scraping.spiders.homzmart_delete_tables import DeleteTables

# Define the DAG
default_args = {
    'owner': 'oelmaria',
    'email': ['omar.elmaria@kemitt.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 0.1), # 6 seconds
    'start_date': pendulum.datetime(date.today().year, date.today().month, date.today().day),
    'depends_on_past': False, 
}
dag = DAG(dag_id = 'dag_homzmart_scraping', default_args = default_args, schedule_interval = None, catchup = False) # Catchup = False --> Ignore untriggered DAG runs between the start date and today

# Define the Python operators which will run the scripts
home_page_task = PythonOperator(
    task_id = 'home_page',
    python_callable = home_page_spider,
    dag = dag
)

file_sensor_task_home_page = FileSensor(
    task_id = 'file_sense_home_page',
    fs_conn_id = 'json_files_directory', # Must be configured in the Airflow UI via Admin --> Connections
    filepath = '/opt/airflow/data/Output_Home_Page.json',
    mode = 'poke', # Keep checking until true or a timeout occurs
    poke_interval = 60, # Check for the existence of the JSON file every 60 seconds
    timeout = 300, # Fail the task if the file does not exist after 5 minutes (i.e., 5 trials)
    dag = dag
)

cat_page_task = PythonOperator(
    task_id = 'cat_page',
    python_callable = cat_page_spider,
    dag = dag
)

file_sensor_task_cat_page = FileSensor(
    task_id = 'file_sense_cat_page',
    fs_conn_id = 'json_files_directory',
    filepath = '/opt/airflow/data/Output_Cat_Page.json',
    mode = 'poke',
    poke_interval = 60,
    timeout = 300, 
    dag = dag
)

subcat_page_task = PythonOperator(
    task_id = 'subcat_page',
    python_callable = subcat_page_spider,
    dag = dag
)

file_sensor_task_subcat_page = FileSensor(
    task_id = 'file_sense_subcat_page',
    fs_conn_id = 'json_files_directory',
    filepath = '/opt/airflow/data/Output_SubCat_Page.json',
    mode = 'poke',
    poke_interval = 60,
    timeout = 300, 
    dag = dag
)

prod_page_task = PythonOperator(
    task_id = 'prod_page',
    python_callable = prod_page_spider,
    dag = dag
)

file_sensor_task_prod_page = FileSensor(
    task_id = 'file_sense_prod_page',
    fs_conn_id = 'json_files_directory',
    filepath = '/opt/airflow/data/Output_Prod_Page.json',
    mode = 'poke',
    poke_interval = 60,
    timeout = 300, 
    dag = dag
)

combine_jsons_task = PythonOperator(
    task_id = 'combine_jsons',
    python_callable = combine_jsons,
    dag = dag
)

# To see how to send emails via Airflow, check these two blog posts --> https://naiveskill.com/send-email-from-airflow/ + https://stackoverflow.com/questions/58736009/email-on-failure-retry-with-airflow-in-docker-container
success_email_body = f'The homzmart scraping DAG has been successfully executed at {datetime.now()}'
send_email_task = EmailOperator(
    task_id = 'send_email',
    to = ['<omar.elmaria@kemitt.com>'], # The <> are important. Don't forget to include them. You can add more emails to the list
    subject = "The Airflow DAG Run of Homzmart's Crawler Has Been Successfully Executed",
    html_content = success_email_body,
    dag = dag
)

# This task is NOT needed in the normal DAG run
# delete_tables_task = PythonOperator(
#     task_id = 'delete_tables',
#     python_callable = delete_tables,
#     dag = dag
# )

# Set the order of the tasks
home_page_task >> file_sensor_task_home_page >> cat_page_task >> file_sensor_task_cat_page >> subcat_page_task >> file_sensor_task_subcat_page
file_sensor_task_subcat_page >> prod_page_task >> file_sensor_task_prod_page >> combine_jsons_task >> send_email_task # Completing the dependency definitions in a second line so that the first line doesn't become too long