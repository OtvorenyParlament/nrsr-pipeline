"""
NRSR.sk pipeline
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators import NRSRScrapyOperator, NRSRTransformOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Jozef Sukovsky',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2018, 10, 9)
}

DAILY = True
PERIOD = 7
POSTGRES_URL = Variable.get('postgres_url')
MONGO_SETTINGS = Variable.get('mongo_settings', deserialize_json=True)

dag = DAG('NRSRPipeline', default_args=default_args)


def dummy():
    return True


# extract data from nrsr.sk
extract_members = NRSRScrapyOperator(
    task_id='extract_members',
    spider='members',
    daily=DAILY,
    period=PERIOD,
    dag=dag)

extract_member_changes = NRSRScrapyOperator(
    task_id='extract_member_changes',
    spider='member_changes',
    daily=DAILY,
    period=PERIOD,
    dag=dag
)

extract_missing_members = NRSRScrapyOperator(
    task_id='extract_missing_members',
    spider='missing_members',
    daily=DAILY,
    period=PERIOD,
    dag=dag
)

# extract_sessions = NRSRScrapyOperator(
#     task_id='extract_sessions',
#     spider='sessions',
#     daily=DAILY,
#     period=PERIOD,
#     dag=dag
# )

# extract_presses = NRSRScrapyOperator(
#     task_id='extract_presses',
#     spider='presses',
#     daily=DAILY,
#     period=PERIOD,
#     dag=dag
# )

# extract_clubs = NRSRScrapyOperator(
#     task_id='extract_clubs',
#     spider='clubs',
#     daily=DAILY,
#     period=PERIOD,
#     dag=dag)

# extract_votings = NRSRScrapyOperator(
#     task_id='extract_votings',
#     spider='votings',
#     daily=DAILY,
#     period=PERIOD,
#     dag=dag
# )


# extract_hour_of_questions = NRSRScrapyOperator(
#     task_id='extract_hour_of_questions',
#     spider='hqa',
#     daily=DAILY,
#     period=PERIOD,
#     dag=dag
# )

# extract_debate_appearances = NRSRScrapyOperator(
#     task_id='extract_debate_appearances',
#     spider='debate_appearances',
#     daily=DAILY,
#     period=PERIOD,
#     dag=dag
# )

# extract_draft_law = NRSRScrapyOperator(
#     task_id='extract_draft_law',
#     spider='draft_law',
#     daily=DAILY,
#     period=PERIOD,
#     dag=dag
# )

# extract_interpelations = NRSRScrapyOperator(
#     task_id='extract_interpelations',
#     spider='interpelations',
#     daily=DAILY,
#     period=PERIOD,
#     dag=dag
# )

# transform data

# transform_clubs = PythonOperator(
#     task_id='transform_clubs',
#     python_callable=dummy,
#     dag=dag
# )

transform_members = NRSRTransformOperator(
    task_id='transform_members',
    data_type='member',
    period=PERIOD,
    daily=DAILY,
    postgres_url=POSTGRES_URL,
    mongo_settings=MONGO_SETTINGS,
    dag=dag
)

# transform_member_changes = PythonOperator(
#     task_id='transform_member_changes',
#     python_callable=dummy,
#     dag=dag
# )

# transform_presses = PythonOperator(
#     task_id='transform_presses',
#     python_callable=dummy,
#     dag=dag
# )

# transform_sessions = PythonOperator(
#     task_id='transform_sessions',
#     python_callable=dummy,
#     dag=dag
# )

# transform_votings = PythonOperator(
#     task_id='transform_votings',
#     python_callable=dummy,
#     dag=dag
# )

# load data
load_members = PythonOperator(
    task_id='load_members',
    python_callable=dummy,
    dag=dag
)

# load_member_changes = PythonOperator(
#     task_id='load_member_changes',
#     python_callable=dummy,
#     dag=dag
# )

# load_clubs = PythonOperator(
#     task_id='load_clubs',
#     python_callable=dummy,
#     dag=dag
# )

# load_sessions = PythonOperator(
#     task_id='load_sessions',
#     python_callable=dummy,
#     dag=dag
# )

# load_presses = PythonOperator(
#     task_id='load_presses',
#     python_callable=dummy,
#     dag=dag
# )

# load_votings = PythonOperator(
#     task_id='load_votings',
#     python_callable=dummy,
#     dag=dag
# )


# task dependencies
extract_member_changes.set_upstream(extract_members)
extract_missing_members.set_upstream(extract_member_changes)
transform_members.set_upstream(extract_members)
transform_members.set_upstream(extract_missing_members)
load_members.set_upstream(transform_members)

# transform_member_changes.set_upstream(extract_member_changes)
# load_member_changes.set_upstream(transform_member_changes)
# load_member_changes.set_upstream(load_members)

# extract_sessions.set_upstream(extract_missing_members)
# transform_sessions.set_upstream(extract_sessions)
# load_sessions.set_upstream(transform_sessions)

# extract_presses.set_upstream(extract_sessions)
# transform_presses.set_upstream(extract_presses)
# load_presses.set_upstream(transform_presses)


# extract_clubs.set_upstream(extract_presses)
# transform_clubs.set_upstream(extract_clubs)
# transform_clubs.set_upstream(load_members)
# load_clubs.set_upstream(transform_clubs)


# extract_votings.set_upstream(extract_clubs)
# transform_votings.set_upstream(extract_votings)
# load_votings.set_upstream(transform_votings)
# load_votings.set_upstream(load_members)
# load_votings.set_upstream(load_sessions)
# load_votings.set_upstream(load_presses)
