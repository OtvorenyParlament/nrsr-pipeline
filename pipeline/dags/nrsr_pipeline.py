"""
NRSR.sk pipeline
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators import NRSRLoadOperator, NRSRScrapyOperator, NRSRTransformOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Jozef Sukovsky',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2018, 10, 10)
}

DAILY = True
PERIOD = 7
POSTGRES_URL = Variable.get('postgres_url')
MONGO_SETTINGS = Variable.get('mongo_settings', deserialize_json=True)
TRANSFORMED_DST = '/tmp'
SCRAPY_HOME = Variable.get('scrapy_home')

dag = DAG('NRSRPipeline', default_args=default_args)


def dummy():
    return True


# extract data from nrsr.sk
extract_members = NRSRScrapyOperator(
    task_id='extract_members',
    spider='members',
    scrapy_home=SCRAPY_HOME,
    daily=DAILY,
    period=PERIOD,
    dag=dag)

extract_member_changes = NRSRScrapyOperator(
    task_id='extract_member_changes',
    spider='member_changes',
    scrapy_home=SCRAPY_HOME,
    daily=DAILY,
    period=PERIOD,
    dag=dag
)

extract_missing_members = NRSRScrapyOperator(
    task_id='extract_missing_members',
    spider='missing_members',
    scrapy_home=SCRAPY_HOME,
    daily=DAILY,
    period=PERIOD,
    dag=dag
)

extract_presses = NRSRScrapyOperator(
    task_id='extract_presses',
    spider='presses',
    scrapy_home=SCRAPY_HOME,
    daily=DAILY,
    period=PERIOD,
    dag=dag
)

extract_sessions = NRSRScrapyOperator(
    task_id='extract_sessions',
    spider='sessions',
    scrapy_home=SCRAPY_HOME,
    daily=DAILY,
    period=PERIOD,
    dag=dag
)

extract_clubs = NRSRScrapyOperator(
    task_id='extract_clubs',
    spider='clubs',
    scrapy_home=SCRAPY_HOME,
    daily=DAILY,
    period=PERIOD,
    dag=dag)

extract_votings = NRSRScrapyOperator(
    task_id='extract_votings',
    spider='votings',
    scrapy_home=SCRAPY_HOME,
    daily=DAILY,
    period=PERIOD,
    dag=dag
)


# extract_hour_of_questions = NRSRScrapyOperator(
#     task_id='extract_hour_of_questions',
#     spider='hqa',
#     scrapy_home=SCRAPY_HOME,
#     daily=DAILY,
#     period=PERIOD,
#     dag=dag
# )

# extract_debate_appearances = NRSRScrapyOperator(
#     task_id='extract_debate_appearances',
#     spider='debate_appearances',
#     scrapy_home=SCRAPY_HOME,
#     daily=DAILY,
#     period=PERIOD,
#     dag=dag
# )

# extract_draft_law = NRSRScrapyOperator(
#     task_id='extract_draft_law',
#     spider='draft_law',
#     scrapy_home=SCRAPY_HOME,
#     daily=DAILY,
#     period=PERIOD,
#     dag=dag
# )

# extract_interpelations = NRSRScrapyOperator(
#     task_id='extract_interpelations',
#     spider='interpelations',
#     scrapy_home=SCRAPY_HOME,
#     daily=DAILY,
#     period=PERIOD,
#     dag=dag
# )

# transform data

transform_members = NRSRTransformOperator(
    task_id='transform_members',
    data_type='member',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    file_dest=TRANSFORMED_DST,
    dag=dag
)

transform_member_changes = NRSRTransformOperator(
    task_id='transform_member_changes',
    data_type='member_change',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    file_dest=TRANSFORMED_DST,
    dag=dag
)

transform_presses = NRSRTransformOperator(
    task_id='transform_presses',
    data_type='press',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    file_dest=TRANSFORMED_DST,
    dag=dag
)

transform_sessions = NRSRTransformOperator(
    task_id='transform_sessions',
    data_type='session',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    file_dest=TRANSFORMED_DST,
    dag=dag
)

transform_clubs = NRSRTransformOperator(
    task_id='transform_clubs',
    data_type='club',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    file_dest=TRANSFORMED_DST,
    dag=dag
)


transform_votings = NRSRTransformOperator(
    task_id='transform_votings',
    data_type='voting',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    file_dest=TRANSFORMED_DST,
    dag=dag
)

# load data
load_members = NRSRLoadOperator(
    task_id='load_members',
    data_type='member',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    file_src=TRANSFORMED_DST,
    dag=dag
)

load_member_changes = NRSRLoadOperator(
    task_id='load_member_changes',
    data_type='member_change',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    file_src=TRANSFORMED_DST,
    dag=dag
)

load_presses = NRSRLoadOperator(
    task_id='load_presses',
    data_type='press',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    file_src=TRANSFORMED_DST,
    dag=dag
)

load_sessions = NRSRLoadOperator(
    task_id='load_sessions',
    data_type='session',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    file_src=TRANSFORMED_DST,
    dag=dag
)

load_clubs = NRSRLoadOperator(
    task_id='load_clubs',
    data_type='club',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    file_src=TRANSFORMED_DST,
    dag=dag
)

load_votings = NRSRLoadOperator(
    task_id='load_votings',
    data_type='voting',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    file_src=TRANSFORMED_DST,
    dag=dag
)


# task dependencies
extract_member_changes.set_upstream(extract_members)
extract_missing_members.set_upstream(extract_member_changes)
transform_members.set_upstream(extract_members)
transform_members.set_upstream(extract_missing_members)
load_members.set_upstream(transform_members)

transform_member_changes.set_upstream(extract_member_changes)
load_member_changes.set_upstream(transform_member_changes)
load_member_changes.set_upstream(load_members)

extract_presses.set_upstream(extract_missing_members)
transform_presses.set_upstream(extract_presses)
load_presses.set_upstream(transform_presses)

extract_sessions.set_upstream(extract_presses)
transform_sessions.set_upstream(extract_sessions)
load_sessions.set_upstream(load_presses)
load_sessions.set_upstream(transform_sessions)


extract_clubs.set_upstream(extract_sessions)
transform_clubs.set_upstream(extract_clubs)
transform_clubs.set_upstream(load_members)
load_clubs.set_upstream(transform_clubs)


extract_votings.set_upstream(extract_clubs)
transform_votings.set_upstream(extract_votings)
load_votings.set_upstream(transform_votings)
load_votings.set_upstream(load_members)
load_votings.set_upstream(load_sessions)
load_votings.set_upstream(load_presses)
