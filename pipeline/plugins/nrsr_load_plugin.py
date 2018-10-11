"""
Data Loader
"""

from datetime import datetime, timedelta
import logging
import os

import pandas
import psycopg2

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin


class NRSRLoadOperator(BaseOperator):
    """
    Load Data
    """

    def __init__(self, data_type, period, daily, postgres_url, file_src, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.data_type = data_type
        self.period = period
        self.daily = daily
        self.file_src = file_src
        self.data_type = data_type

        self.postgres_url = postgres_url

    def load_members(self):
        """
        Load MPs
        """
        data_frame = pandas.read_csv('{}/{}.csv'.format(self.file_src, self.data_type))
        if not data_frame.empty:
            pg_conn = None
            try:
                pg_conn = psycopg2.connect(self.postgres_url)
                pg_cursor = pg_conn.cursor()

                for _, row in data_frame.iterrows():
                    pg_cursor.execute(
                        """
                        INSERT INTO
                            person_person (title, forename, surname, born, email, nationality, external_photo_url, external_id, residence_id)
                        VALUES
                            (
                            '{title}',
                            '{forename}',
                            '{surname}',
                            '{born}',
                            '{email}',
                            '{nationality}',
                            '{external_photo_url}',
                            {external_id},
                            {residence_id}
                        )
                        ON CONFLICT (external_id) DO NOTHING;

                        INSERT INTO parliament_party (name) VALUES ('{stood_for_party}') ON CONFLICT (name) DO NOTHING;

                        INSERT INTO
                            parliament_member (period_id, person_id, stood_for_party_id, url)
                        VALUES (
                                (SELECT id FROM parliament_period where period_num = {period_num}),
                                (SELECT id FROM person_person WHERE external_id = {external_id}),
                                (SELECT id FROM parliament_party WHERE name = '{stood_for_party}'),
                                '{url}'
                        );
                        """.format(**row)
                    )
                pg_conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                raise error
            finally:
                if pg_conn is not None:
                    pg_conn.close()


    def execute(self, context):
        """Operator Executor"""
        if self.data_type == 'member':
            self.load_members()


class NRSRLoadPlugin(AirflowPlugin):
    
    name = 'nrsr_load_plugin'
    operators = [NRSRLoadOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
