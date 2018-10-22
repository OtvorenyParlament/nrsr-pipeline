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
        self.data_frame = pandas.DataFrame([])

        self.postgres_url = postgres_url

    def load_members(self):
        """
        Load MPs
        """

        if not self.data_frame.empty:
            pg_conn = None
            try:
                pg_conn = psycopg2.connect(self.postgres_url)
                pg_cursor = pg_conn.cursor()

                for _, row in self.data_frame.iterrows():
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
                        )
                        ON CONFLICT DO NOTHING;
                        """.format(**row)
                    )
                pg_conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                raise error
            finally:
                if pg_conn is not None:
                    pg_conn.close()

    def load_member_changes(self):
        """
        Load MP changes
        """

        if not self.data_frame.empty:
            pg_conn = None
            try:
                pg_conn = psycopg2.connect(self.postgres_url)
                pg_cursor = pg_conn.cursor()

                for _, row in self.data_frame.iterrows():
                    pg_cursor.execute(
                        """
                        INSERT INTO parliament_memberchange ("date", change_type, change_reason, period_id, person_id)
                        VALUES (
                            '{date}',
                            '{change_type}',
                            '{change_reason}',
                            (SELECT id FROM parliament_period WHERE period_num = {period_num}),
                            (SELECT id FROM person_person WHERE external_id = {external_id})
                        )
                        ON CONFLICT (person_id, period_id, date, change_type) DO NOTHING;
                        """.format(**row)
                    )
                
                # TODO(Jozef): The following weak and dirty solution should be refactored
                pg_cursor.execute(
                """
                SELECT MC.person_id, MC.date, MC.change_type FROM parliament_memberchange MC
                INNER JOIN parliament_period P ON MC.period_id = P.id
                WHERE P.period_num = {period_num}
                ORDER BY MC.person_id, MC."date" ASC;
                """.format(period_num=self.period))
                rows = pg_cursor.fetchall()
                persons = {}
                for row in rows:
                    if not row[0] in persons:
                        persons[row[0]] = {}
                    if not row[1] in persons[row[0]]:
                        persons[row[0]][row[1]] = []
                    persons[row[0]][row[1]].append(row[2])
                    
                reverse_pairs = [['doesnotapply', 'active'], ['substituteactive', 'substitutegained']]
                for key, val in persons.items():
                    for key2, val2 in val.items():
                        if val2 in reverse_pairs:
                            val2.reverse()

                personline = {}
                for key, val in persons.items():
                    if not key in personline:
                        personline[key] = []
                    for key2, val2 in val.items():
                        for val3 in val2:
                            if val3 in ['active', 'substituteactive']:
                                personline[key].append([key2, 'on'])
                            elif val3 in ['doesnotapply', 'folded', 'substitutefolded']:
                                personline[key].append([key2, 'off'])

                pg_cursor.execute(
                    """
                    SELECT M.id, M.person_id FROM parliament_member M
                    INNER JOIN parliament_period P
                    ON M.period_id = P.id
                    WHERE P.period_num = {period_num}
                    """.format(period_num=self.period)
                )
                member_rows = pg_cursor.fetchall()
                member_pairs = {x[1]: x[0] for x in member_rows}

                records = []
                for key, val in personline.items():
                    val_len = len(val)
                    for i in range(0, val_len, 2):
                        try:
                            records.append({
                                'member_id': member_pairs[key],
                                'start': val[i][0].strftime('%Y-%m-%d'),
                                'end': val[i+1][0].strftime('%Y-%m-%d')
                            })
                        except IndexError:
                            records.append({
                                'member_id': member_pairs[key],
                                'start': val[i][0].strftime('%Y-%m-%d'),
                                'end': None
                            })

                for record in records:
                    if record['end']:
                        pg_cursor.execute(
                            """
                            INSERT INTO parliament_memberactive (member_id, start, "end")
                            VALUES ({member_id}, '{start}', '{end}')
                            """.format(**record)
                        )
                    else:
                        pg_cursor.execute(
                            """
                            INSERT INTO parliament_memberactive (member_id, start)
                            VALUES ({member_id}, '{start}')
                            """.format(**record)
                        )
                pg_conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                raise error
            finally:
                if pg_conn is not None:
                    pg_conn.close()

    def load_presses(self):
        """
        Load presses
        """
        if not self.data_frame.empty:
            pg_conn = None
            try:
                pg_conn = psycopg2.connect(self.postgres_url)
                pg_cursor = pg_conn.cursor()

                for _, row in self.data_frame.iterrows():
                    row['title'] = row['title'].replace("'", "''")
                    pg_cursor.execute(
                        """
                        INSERT INTO parliament_press (press_type, title, press_num, "date", url, period_id)
                        VALUES (
                            '{press_type}',
                            '{title}',
                            {press_num},
                            '{date}',
                            '{url}',
                            (SELECT id FROM parliament_period WHERE period_num = {period_num})
                        )
                        ON CONFLICT (press_num, period_id) DO NOTHING;
                        """.format(**row)
                    )
                pg_conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                raise error
            finally:
                if pg_conn is not None:
                    pg_conn.close()

    def load_sessions(self):
        """
        Load Sessions and program
        """
        if not self.data_frame.empty:
            pg_conn = None
            try:
                pg_conn = psycopg2.connect(self.postgres_url)
                pg_cursor = pg_conn.cursor()
                self.data_frame = self.data_frame.where(self.data_frame.notnull(), 'null')

                for _, row in self.data_frame.iterrows():
                    if row['parlpress'] != 'null':
                        row['parlpress'] = int(row['parlpress'])
                    if row['progpoint'] != 'null':
                        row['progpoint'] = int(row['progpoint'])
                    if row['text1'] == 'null':
                        row['text1'] = ''
                    else:
                        row['text1'] = row['text1'].replace("'", "''")
                    if row['text2'] == 'null':
                        row['text2'] = ''
                    else:
                        row['text2'] = row['text2'].replace("'", "''")
                    if row['text3'] == 'null':
                        row['text3'] = ''
                    else:
                        row['text3'] = row['text3'].replace("'", "''")
                    query = """

                        INSERT INTO parliament_session("name", external_id, period_id, session_num, url)
                        VALUES (
                            '{name}',
                            {external_id},
                            (SELECT id FROM parliament_period WHERE period_num = {period_num}),
                            {session_num},
                            '{url}'
                        )
                        ON CONFLICT(external_id) DO NOTHING;

                        INSERT INTO parliament_sessionprogram(session_id, press_id, point, state, text1, text2, text3)
                        VALUES (
                            (SELECT id FROM parliament_session WHERE external_id = {external_id}),
                            (SELECT PR.id FROM parliament_press PR 
                            INNER JOIN parliament_period PE ON PR.period_id = PE.id 
                            WHERE PE.period_num = {period_num}
                            AND PR.press_num = '{parlpress}'),
                            {progpoint},
                            '{state}',
                            '{text1}',
                            '{text2}',
                            '{text3}'
                        )
                        ON CONFLICT DO NOTHING;

                    """.format(**row)
                    pg_cursor.execute(query)
                pg_conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                raise error
            finally:
                if pg_conn is not None:
                    pg_conn.close()

    def load_clubs(self):
        """
        Load Clubs and Club Members
        """
        if not self.data_frame.empty:
            pg_conn = None
            try:
                pg_conn = psycopg2.connect(self.postgres_url)
                pg_cursor = pg_conn.cursor()
                self.data_frame = self.data_frame.where(self.data_frame.notnull(), 'null')

                for _, row in self.data_frame.iterrows():
                    query = """
                    INSERT INTO parliament_club (period_id, name, email, external_id, url)
                    VALUES (
                        (SELECT id FROM parliament_period WHERE period_num = {period_num}),
                        '{name}',
                        '{email}',
                        {external_id},
                        '{url}'
                    )
                    ON CONFLICT DO NOTHING;

                    INSERT INTO parliament_clubmember (club_id, member_id, membership)
                    VALUES (
                        (SELECT id FROM parliament_club WHERE external_id = {external_id}),
                        (SELECT M.id FROM parliament_member M
                        INNER JOIN parliament_period P ON M.period_id = P.id
                        INNER JOIN person_person PER ON PER.id = M.person_id
                        WHERE P.period_num = 7
                        AND PER.external_id = {member_external_id}),
                        '{membership}'
                    )
                    ON CONFLICT DO NOTHING;
                    """.format(**row)
                    pg_cursor.execute(query)
                pg_conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                raise error
            finally:
                if pg_conn is not None:
                    pg_conn.close()

    def load_votings(self):
        """Load Votings and Votes"""
        if not self.data_frame.empty:
            pg_conn = None
            try:
                pg_conn = psycopg2.connect(self.postgres_url)
                pg_cursor = pg_conn.cursor()
                self.data_frame = self.data_frame.where(self.data_frame.notnull(), 'null')
                for _, row in self.data_frame.iterrows():
                    query = """
                    INSERT INTO parliament_voting(external_id, session_id, press_id, voting_num, topic, timestamp, result, url)
                    VALUES (
                        {external_id},
                        (
                            SELECT S.id FROM parliament_session S
                            INNER JOIN parliament_period P ON S.period_id = P.id
                            WHERE P.period_num = {period_num}
                            AND S.session_num = {session_num}
                        ),
                        (
                            SELECT P.id FROM parliament_press P
                            INNER JOIN parliament_period PE ON P.period_id = PE.id
                            WHERE PE.period_num = {period_num}
                            AND P.press_num = '{press_num}'
                        ),
                        {voting_num},
                        '{topic}',
                        '{datetime}',
                        '{result}',
                        '{url}'
                    ) ON CONFLICT DO NOTHING;
                    INSERT INTO parliament_votingvote(voting_id, person_id, vote)
                    VALUES (
                        (SELECT id FROM parliament_voting WHERE external_id = {external_id}),
                        (SELECT id FROM person_person WHERE external_id = {member_external_id}),
                        '{vote}'
                    ) ON CONFLICT DO NOTHING;
                    """.format(**row)
                    pg_cursor.execute(query)
                pg_conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                raise error
            finally:
                if pg_conn is not None:
                    pg_conn.close()

    def execute(self, context):
        """Operator Executor"""
        converters = {
            'member': {},
            'member_change': {},
            'press': {},
            'session': {},
            'club': {},
            'voting': {}
        }

        self.data_frame = pandas.read_csv(
            '{}/{}.csv'.format(self.file_src, self.data_type),
            converters=converters[self.data_type])
        if self.data_type == 'member':
            self.load_members()
        elif self.data_type == 'member_change':
            self.load_member_changes()
        elif self.data_type == 'press':
            self.load_presses()
        elif self.data_type == 'session':
            self.load_sessions()
        elif self.data_type == 'club':
            self.load_clubs()
        elif self.data_type == 'voting':
            self.load_votings()


class NRSRLoadPlugin(AirflowPlugin):

    name = 'nrsr_load_plugin'
    operators = [NRSRLoadOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
