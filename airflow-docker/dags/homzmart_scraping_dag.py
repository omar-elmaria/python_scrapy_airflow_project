# Import the standard Airflow libraries
from airflow.models import DAG
from datetime import datetime, date, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator
from pyparsing import Combine
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

cat_page_task = PythonOperator(
    task_id = 'cat_page',
    python_callable = cat_page_spider,
    dag = dag
)

subcat_page_task = PythonOperator(
    task_id = 'subcat_page',
    python_callable = subcat_page_spider,
    dag = dag
)

prod_page_task = PythonOperator(
    task_id = 'prod_page',
    python_callable = prod_page_spider,
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
home_page_task >> cat_page_task >> subcat_page_task >> prod_page_task >> combine_jsons_task >> send_email_task