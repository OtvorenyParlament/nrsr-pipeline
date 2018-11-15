"""
NRSR.sk pipeline
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators import NRSRLoadOperator, NRSRScrapyOperator, NRSRTransformOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Jozef Sukovsky',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2018, 10, 10),
    'schedule_interval': '0 0 * * 1-5',
}

DAILY = True
PERIOD = 7
POSTGRES_URL = Variable.get('postgres_url')
MONGO_SETTINGS = Variable.get('mongo_settings', deserialize_json=True)
SCRAPY_HOME = Variable.get('scrapy_home')

dag = DAG('NRSRPipeline', default_args=default_args)


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

extract_missing_presses = NRSRScrapyOperator(
    task_id='extract_missing_presses',
    spider='missing_presses',
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

extract_bills = NRSRScrapyOperator(
    task_id='extract_bills',
    spider='bills',
    scrapy_home=SCRAPY_HOME,
    daily=DAILY,
    period=PERIOD,
    dag=dag
)

extract_debate_appearances = NRSRScrapyOperator(
    task_id='extract_debate_appearances',
    spider='debate_appearances',
    scrapy_home=SCRAPY_HOME,
    daily=DAILY,
    period=PERIOD,
    dag=dag
)

extract_interpellations = NRSRScrapyOperator(
    task_id='extract_interpellations',
    spider='interpellations',
    scrapy_home=SCRAPY_HOME,
    daily=DAILY,
    period=PERIOD,
    dag=dag
)

extract_amendments = NRSRScrapyOperator(
    task_id='extract_amendments',
    spider='amendments',
    scrapy_home=SCRAPY_HOME,
    daily=DAILY,
    period=PERIOD,
    dag=dag
)

# transform data

transform_members = NRSRTransformOperator(
    task_id='transform_members',
    data_type='member',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

transform_member_changes = NRSRTransformOperator(
    task_id='transform_member_changes',
    data_type='member_change',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

transform_presses = NRSRTransformOperator(
    task_id='transform_presses',
    data_type='press',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

transform_sessions = NRSRTransformOperator(
    task_id='transform_sessions',
    data_type='session',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

transform_votings = NRSRTransformOperator(
    task_id='transform_votings',
    data_type='voting',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

# transform_clubs = NRSRTransformOperator(
#     task_id='transform_clubs',
#     data_type='club',
#     period=PERIOD,
#     daily=DAILY,
#     postgres_url=POSTGRES_URL,
#     mongo_settings=MONGO_SETTINGS,
#     dag=dag
# )

transform_club_members = NRSRTransformOperator(
    task_id='transform_club_members',
    data_type='daily_club',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

transform_bills = NRSRTransformOperator(
    task_id='transform_bills',
    data_type='bill',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

# transform_bill_process_steps = NRSRTransformOperator(
#     task_id='transform_bill_process_steps',
#     data_type='bill_step',
#     period=PERIOD,
#     daily=DAILY,
#     postgres_url=POSTGRES_URL,
#     mongo_settings=MONGO_SETTINGS,
#     dag=dag
# )

transform_debate_appearances = NRSRTransformOperator(
    task_id='transform_debate_appearances',
    data_type='debate_appearance',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

transform_interpellations = NRSRTransformOperator(
    task_id='transform_interpellations',
    data_type='interpellation',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

transform_amendments = NRSRTransformOperator(
    task_id='transform_amendments',
    data_type='amendment',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

# load data
load_members = NRSRLoadOperator(
    task_id='load_members',
    data_type='member',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

load_member_changes = NRSRLoadOperator(
    task_id='load_member_changes',
    data_type='member_change',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

load_presses = NRSRLoadOperator(
    task_id='load_presses',
    data_type='press',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

load_sessions = NRSRLoadOperator(
    task_id='load_sessions',
    data_type='session',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

load_votings = NRSRLoadOperator(
    task_id='load_votings',
    data_type='voting',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

load_club_members = NRSRLoadOperator(
    task_id='load_club_members',
    data_type='daily_club',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

load_bills = NRSRLoadOperator(
    task_id='load_bills',
    data_type='bill',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

load_debate_appearances = NRSRLoadOperator(
    task_id='load_debate_appearances',
    data_type='debate_appearance',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

load_interpellations = NRSRLoadOperator(
    task_id='load_interpellations',
    data_type='interpellation',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

load_amendments = NRSRLoadOperator(
    task_id='load_amendments',
    data_type='amendment',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

wait_for_loads = DummyOperator(
    task_id='wait_for_loads',
    dag=dag
)

aggregate = DummyOperator(
    task_id='aggregate',
    dag=dag
)

# extracts
extract_member_changes.set_upstream(extract_members)
extract_missing_members.set_upstream(extract_member_changes)
extract_sessions.set_upstream(extract_missing_members)

extract_votings.set_upstream(extract_sessions)

extract_presses.set_upstream(extract_votings)
extract_missing_presses.set_upstream(extract_presses)

extract_bills.set_upstream(extract_missing_presses)

extract_debate_appearances.set_upstream(extract_bills)

extract_interpellations.set_upstream(extract_debate_appearances)

extract_amendments.set_upstream(extract_interpellations)

# transforms
transform_members.set_upstream(extract_members)
transform_members.set_upstream(extract_missing_members)

transform_member_changes.set_upstream(extract_member_changes)

transform_presses.set_upstream(extract_missing_presses)

transform_sessions.set_upstream(extract_sessions)

transform_votings.set_upstream(extract_votings)

transform_club_members.set_upstream(extract_votings)
transform_club_members.set_upstream(load_member_changes)

transform_bills.set_upstream(extract_bills)
transform_bills.set_upstream(load_presses)
# transform_bill_process_steps.set_upstream(transform_bills)

transform_debate_appearances.set_upstream(extract_debate_appearances)

transform_interpellations.set_upstream(extract_interpellations)

transform_amendments.set_upstream(extract_amendments)

# loads
load_members.set_upstream(transform_members)

load_member_changes.set_upstream(transform_member_changes)
load_member_changes.set_upstream(load_members)

load_presses.set_upstream(transform_presses)

load_sessions.set_upstream(load_presses)
load_sessions.set_upstream(transform_sessions)

load_votings.set_upstream(transform_votings)
load_votings.set_upstream(load_members)
load_votings.set_upstream(load_sessions)
load_votings.set_upstream(load_presses)

load_club_members.set_upstream(transform_club_members)

load_bills.set_upstream(transform_bills)
load_bills.set_upstream(load_presses)

load_debate_appearances.set_upstream(transform_debate_appearances)
load_debate_appearances.set_upstream(load_members)
load_debate_appearances.set_upstream(load_presses)
load_debate_appearances.set_upstream(load_sessions)

load_interpellations.set_upstream(transform_interpellations)
load_interpellations.set_upstream(load_members)
load_interpellations.set_upstream(load_sessions)
load_interpellations.set_upstream(load_presses)

load_amendments.set_upstream(transform_amendments)
load_amendments.set_upstream(load_members)
load_amendments.set_upstream(load_sessions)
load_amendments.set_upstream(load_presses)


wait_for_loads.set_upstream(load_members)
wait_for_loads.set_upstream(load_member_changes)
wait_for_loads.set_upstream(load_presses)
wait_for_loads.set_upstream(load_sessions)
wait_for_loads.set_upstream(load_votings)
wait_for_loads.set_upstream(load_club_members)
wait_for_loads.set_upstream(load_bills)
wait_for_loads.set_upstream(load_debate_appearances)
wait_for_loads.set_upstream(load_interpellations)
wait_for_loads.set_upstream(load_amendments)

aggregate.set_upstream(wait_for_loads)
