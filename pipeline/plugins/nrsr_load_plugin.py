"""
Data Loader
"""

from datetime import datetime, timedelta
import logging
import os

import pandas
import psycopg2
from pymongo import MongoClient

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin


class NRSRLoadOperator(BaseOperator):
    """
    Load Data
    """

    def __init__(self, data_type, period, daily, postgres_url, mongo_settings,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.data_type = data_type
        self.period = period
        self.daily = daily
        self.data_type = data_type
        self.data_frame = pandas.DataFrame([])

        mongo_client = MongoClient(mongo_settings['uri'])
        mongo_db = mongo_client[mongo_settings['db']]
        self.mongo_outcol = mongo_db[mongo_settings['outcol']]
        self.postgres_url = postgres_url

    def load_members(self):
        """
        Load MPs
        """

        find_query = {'type': self.data_type}
        if self.mongo_outcol.count_documents(find_query) == 0:
            return None

        docs = self.mongo_outcol.find(find_query)
        pg_conn = None
        try:
            pg_conn = psycopg2.connect(self.postgres_url)
            pg_cursor = pg_conn.cursor()
            query = """
                INSERT INTO
                    person_person (title, forename, surname, born, email,
                                   nationality, external_photo_url, external_id, residence_id)
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

                INSERT INTO parliament_party (name) VALUES ('{stood_for_party}')
                ON CONFLICT DO NOTHING;

                INSERT INTO
                    parliament_member (period_id, person_id, stood_for_party_id, url)
                VALUES (
                        (SELECT id FROM parliament_period where period_num = {period_num}),
                        (SELECT id FROM person_person WHERE external_id = {external_id}),
                        (SELECT id FROM parliament_party WHERE name = '{stood_for_party}'),
                        '{url}'
                )
                ON CONFLICT DO NOTHING;
            """
            for doc in docs:
                pg_cursor.execute(query.format(**doc))
            pg_conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            raise error
        finally:
            if pg_conn is not None:
                pg_conn.close()


    # TODO(Jozef): high cyclomatic complexity. reduce
    def load_member_changes(self):
        """
        Load MP changes
        """

        find_query = {'type': self.data_type}
        if self.mongo_outcol.count_documents(find_query) == 0:
            return None

        docs = self.mongo_outcol.find(find_query)
        pg_conn = None
        try:
            pg_conn = psycopg2.connect(self.postgres_url)
            pg_cursor = pg_conn.cursor()

            # insert into parliament_memberchange
            query = """
                INSERT INTO parliament_memberchange ("date", change_type, change_reason, period_id, person_id)
                VALUES (
                    '{date}',
                    '{change_type}',
                    '{change_reason}',
                    (SELECT id FROM parliament_period WHERE period_num = {period_num}),
                    (SELECT id FROM person_person WHERE external_id = {external_id})
                )
                ON CONFLICT (person_id, period_id, date, change_type) DO NOTHING;
            """
            for doc in docs:
                pg_cursor.execute(query.format(**doc))

            # insert into parliament_memberactive
            doesnotapply = 0
            active = 1
            substituteactive = 3
            substitutegained = 4
            folded = 5
            gained = 6
            substitutefolded = 2
            on = 'on'
            off = 'off'

            pg_cursor.execute(
            """
            SELECT MC.person_id, MC.date, MC.change_type FROM parliament_memberchange MC
            INNER JOIN parliament_period P ON MC.period_id = P.id
            WHERE P.period_num = {period_num}
            ORDER BY MC.person_id, MC."date" ASC;
            """.format(period_num=self.period))
            rows = pg_cursor.fetchall()
            print("Rows count: {}".format(len(rows)))
            persons = {}
            for row in rows:
                if not row[0] in persons:
                    persons[row[0]] = {}
                if not row[1] in persons[row[0]]:
                    persons[row[0]][row[1]] = []
                persons[row[0]][row[1]].append(row[2])

            reverse_pairs = [[doesnotapply, active], [substituteactive, substitutegained]]
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
                        if val3 in [active, substituteactive]:
                            personline[key].append([key2, on])
                        elif val3 in [doesnotapply, folded, substitutefolded]:
                            personline[key].append([key2, off])

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
            datestring = '%Y-%m-%d'
            for key, val in personline.items():
                if len(val) == 1 and val[0][1] == off:
                    continue
                val_len = len(val)
                for i in range(0, val_len, 2):
                    if val[i][1] == off and val[i-1][1] == off:
                        continue
                    try:
                        records.append({
                            'member_id': member_pairs[key],
                            'start': val[i][0].strftime(datestring),
                            'end': val[i+1][0].strftime(datestring)
                        })
                    except IndexError:
                        records.append({
                            'member_id': member_pairs[key],
                            'start': val[i][0].strftime(datestring),
                            'end': None
                        })

            for record in records:
                if record['end']:
                    pg_cursor.execute(
                        """
                        INSERT INTO parliament_memberactive (member_id, start, "end")
                        VALUES ({member_id}, '{start}', '{end}')
                        ON CONFLICT (member_id, start) WHERE "end" IS NULL DO UPDATE SET "end" = '{end}';
                        """.format(**record)
                    )
                else:
                    pg_cursor.execute(
                        """
                        INSERT INTO parliament_memberactive (member_id, start)
                        VALUES ({member_id}, '{start}')
                        ON CONFLICT DO NOTHING;
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
        find_query = {'type': self.data_type}
        if self.mongo_outcol.count_documents(find_query) == 0:
            return None

        docs = self.mongo_outcol.find(find_query)
        pg_conn = None
        try:
            pg_conn = psycopg2.connect(self.postgres_url)
            pg_cursor = pg_conn.cursor()
            query = """
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
            """
            for doc in docs:
                doc['title'] = doc['title'].replace("'", "''")
                pg_cursor.execute(query.format(**doc))
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
        find_query = {'type': self.data_type}
        if self.mongo_outcol.count_documents(find_query) == 0:
            return None

        docs = self.mongo_outcol.find(find_query)
        pg_conn = None
        try:
            pg_conn = psycopg2.connect(self.postgres_url)
            pg_cursor = pg_conn.cursor()
            session_query = """
                INSERT INTO parliament_session("name", external_id, period_id, session_num, url)
                VALUES (
                    '{name}',
                    {external_id},
                    (SELECT id FROM parliament_period WHERE period_num = {period_num}),
                    {session_num},
                    '{url}'
                )
                ON CONFLICT(external_id) DO NOTHING;
            """

            point_query = """
                INSERT INTO parliament_sessionprogram(session_id, press_id, point, state, text1, text2, text3)
                VALUES (
                    (SELECT id FROM parliament_session WHERE external_id = {external_id}),
                    (SELECT PR.id FROM parliament_press PR 
                    INNER JOIN parliament_period PE ON PR.period_id = PE.id 
                    WHERE PE.period_num = {period_num}
                    AND PR.press_num = '{press_num}'),
                    {point_num},
                    {state},
                    '{text1}',
                    '{text2}',
                    '{text3}'
                )
                ON CONFLICT DO NOTHING;
            """
            for doc in docs:
                if not doc['session_num']:
                    doc['session_num'] = 'NULL'
                pg_cursor.execute(session_query.format(**doc))

                if 'program_points' in doc:
                    for point in doc['program_points']:
                        if not point['point_num']:
                            point['point_num'] = 'NULL'
                        if not point['press_num']:
                            point['press_num'] = 'NULL'
                        
                        point['text1'] = point['text1'].replace("'", "''")
                        point['text2'] = point['text2'].replace("'", "''")
                        point['text3'] = point['text3'].replace("'", "''")
                        pg_cursor.execute(
                            point_query.format(
                                external_id=doc['external_id'],
                                period_num=doc['period_num'],
                                **point))
            pg_conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            raise error
        finally:
            if pg_conn is not None:
                pg_conn.close()

    def load_club_members(self):
        """Load Club Members"""
        find_query = {'type': self.data_type}
        if self.mongo_outcol.count_documents(find_query) == 0:
            return None

        docs = self.mongo_outcol.find(find_query)
        distinct_clubs = self.mongo_outcol.aggregate([
            {'$match': find_query},
            {'$group': {'_id': {'period_num': '$period_num', 'club': '$club'}}},
            {'$project': {'club': '$_id.club', 'period_num': '$_id.period_num'}}
        ])
        distinct_periods = self.mongo_outcol.distinct('period_num', find_query)
        pg_conn = None
        try:
            pg_conn = psycopg2.connect(self.postgres_url)
            pg_cursor = pg_conn.cursor()

            # get period ids
            pg_cursor.execute(
                """
                SELECT id, period_num FROM parliament_period WHERE period_num IN ({})
                """.format(', '.join(map(str, distinct_periods)))
            )

            # insert clubs
            club_query = """
                INSERT INTO parliament_club (name, period_id, coalition)
                VALUES (
                    '{club}',
                    (SELECT id FROM parliament_period WHERE period_num = {period_num}),
                    FALSE
                )
                ON CONFLICT DO NOTHING;
            """
            for club in distinct_clubs:
                pg_cursor.execute(club_query.format(**club))

            # insert club members
            clubmember_query = """
                INSERT INTO parliament_clubmember (club_id, member_id, start, "end", membership)
                VALUES (
                    (
                        SELECT C.id FROM parliament_club C
                        INNER JOIN parliament_period P ON C.period_id = P.id
                        WHERE C.name = '{club_name}' AND P.period_num = {period_num}
                    ),
                    (
                        SELECT M.id FROM parliament_member M
                        INNER JOIN parliament_period P ON M.period_id = P.id
                        INNER JOIN person_person S ON M.person_id = S.id
                        WHERE S.external_id = {member_external_id} AND P.period_num = {period_num}
                    ),
                    '{start}',
                    {end},
                    ''
                )
                ON CONFLICT (club_id, member_id, start) {on_conflict};
            """
            for doc in docs:
                insert_doc = {
                    'club_name': doc['club'],
                    'period_num': doc['period_num'],
                    'member_external_id': doc['member_external_id'],
                    'start': doc['start'],
                    'end': """'{}'""".format(doc['end']) if doc['end'] else 'NULL',
                    'on_conflict': """WHERE "end" IS NULL DO UPDATE SET "end" = '{}'""".format(
                        doc['end']) if doc['end'] else 'DO NOTHING'
                }
                pg_cursor.execute(clubmember_query.format(**insert_doc))
            pg_conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            raise error
        finally:
            if pg_conn is not None:
                pg_conn.close()

    def load_votings(self):
        """Load Votings and Votes"""
        find_query = {'type': self.data_type}
        if self.mongo_outcol.count_documents(find_query) == 0:
            return None

        docs = self.mongo_outcol.find(find_query)
        pg_conn = None
        try:
            pg_conn = psycopg2.connect(self.postgres_url)
            pg_cursor = pg_conn.cursor()
            voting_query = """
                INSERT INTO parliament_voting(external_id, session_id, press_id,
                                              voting_num, topic, timestamp, result, url)
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
                    {result},
                    '{url}'
                ) ON CONFLICT DO NOTHING;
            """

            vote_query = """
                INSERT INTO parliament_votingvote(voting_id, voter_id, vote)
                VALUES (
                    (SELECT id FROM parliament_voting WHERE external_id = {voting_external_id}),
                    (
                        SELECT M.id FROM parliament_member M
                        INNER JOIN person_person P ON M.person_id = P.id
                        INNER JOIN parliament_period E ON M.period_id = E.id
                        WHERE P.external_id = {external_id} AND E.period_num = {period_num}
                    ),
                    '{vote}'
                ) ON CONFLICT DO NOTHING;
            """

            for doc in docs:
                doc['topic'] = doc['topic'].replace("'", "''")
                if 'press_num' not in doc or not doc['press_num']:
                    doc['press_num'] = 'NULL'
                pg_cursor.execute(voting_query.format(**doc))

                for vote in doc['votes']:
                    pg_cursor.execute(vote_query.format(
                        voting_external_id=doc['external_id'], period_num=doc['period_num'], **vote))

            pg_conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            raise error
        finally:
            if pg_conn is not None:
                pg_conn.close()

    def load_bills(self):
        """Load Bills"""

        find_query = {'type': self.data_type}
        if self.mongo_outcol.count_documents(find_query) == 0:
            return None

        docs = self.mongo_outcol.find(find_query)
        pg_conn = None
        try:
            pg_conn = psycopg2.connect(self.postgres_url)
            pg_cursor = pg_conn.cursor()
            bill_query = """
            INSERT INTO parliament_bill (
                external_id, delivered, proposer_nonmember,
                state, result, url, press_id, category)
            VALUES (
                {external_id},
                {delivered},
                '{proposer_nonmember}',
                {current_state},
                {current_result},
                '{url}',
                (
                    SELECT S.id FROM parliament_press S
                    INNER JOIN parliament_period P ON S.period_id = P.id
                    WHERE P.period_num = {period_num} AND S.press_num = '{press_num}'
                ),
                {category_name}
            )
            ON CONFLICT DO NOTHING
            """

            proposer_query = """
            INSERT INTO parliament_billproposer (bill_id, member_id)
            VALUES (
                (
                    SELECT id FROM parliament_bill WHERE external_id = {external_id}
                ),
                {member_id}
            )
            ON CONFLICT DO NOTHING
            """

            for doc in docs:
                doc['delivered'] = """'{}'""".format(
                    doc['delivered']) if doc['delivered'] else 'NULL'
                if not 'current_result' in doc or not doc['current_result']:
                    doc['current_result'] = 'NULL'
                if not 'current_state' in doc or not doc['current_state']:
                    doc['current_state'] = 'NULL'
                pg_cursor.execute(bill_query.format(**doc))
                if 'proposers' in doc:
                    for proposer in doc['proposers']:
                        pg_cursor.execute(proposer_query.format(
                            member_id=proposer, external_id=doc['external_id']))

            pg_conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            raise error
        finally:
            if pg_conn is not None:
                pg_conn.close()

    def load_debate_appearances(self):
        """Load debate appearances"""
        find_query = {'type': self.data_type}
        if self.mongo_outcol.count_documents(find_query) == 0:
            return None

        docs = self.mongo_outcol.find(find_query)
        pg_conn = None
        try:
            pg_conn = psycopg2.connect(self.postgres_url)
            pg_cursor = pg_conn.cursor()
            debate_query = """
                INSERT INTO parliament_debateappearance (
                    external_id, "start", "end", appearance_type, video_url, appearance_addition,
                    debater_ext, debater_role, "text", debater_id, session_id
                ) VALUES (
                    {external_id},
                    '{start}',
                    '{end}',
                    {appearance_type},
                    '{video_short_url}',
                    '{appearance_type_addition}',
                    '{debater_ext}',
                    '{debater_role}',
                    '{text}',
                    {debater_id},
                    (
                        SELECT S.id FROM parliament_session S
                        INNER JOIN parliament_period P ON S.period_id = P.id
                        WHERE P.period_num = {period_num}
                        AND S.session_num = {session_num}
                    )
                ) ON CONFLICT DO NOTHING
            """

            press_query = """
                INSERT INTO parliament_debateappearance_press_num (debateappearance_id, press_id)
                VALUES (
                    (
                        SELECT D.id FROM parliament_debateappearance D
                        INNER JOIN parliament_session S ON D.session_id = S.id
                        INNER JOIN parliament_period P ON P.id = S.period_id
                        WHERE P.period_num = {period_num} AND S.session_num = {session_num}
                        AND D.external_id = {external_id}
                    ),
                    (
                        SELECT S.id FROM parliament_press S
                        INNER JOIN parliament_period P ON S.period_id = P.id
                        WHERE P.period_num = {period_num} AND S.press_num = '{press_num}'
                    )
                ) ON CONFLICT DO NOTHING
            """
            for doc in docs:
                doc['text'] = doc['text'].replace("'", "''")
                if doc['parliament_member']:
                    doc['debater_ext'] = ''
                    doc['debater_id'] = """
                    (
                        SELECT M.id FROM parliament_member M
                        INNER JOIN person_person P ON M.person_id = P.id
                        INNER JOIN parliament_period E ON M.period_id = E.id
                         WHERE P.forename = '{debater_forename}' AND
                        P.surname = '{debater_surname}'
                        AND E.period_num = {period_num}
                    )
                    """.format(**doc)
                else:
                    doc['debater_ext'] = '{} {}'.format(
                        doc['debater_forename'], doc['debater_surname'])
                    doc['debater_id'] = 'NULL'
                pg_cursor.execute(debate_query.format(**doc))

                if 'press_num' in doc:
                    for press in doc['press_num']:
                        pg_cursor.execute(press_query.format(
                            period_num=doc['period_num'],
                            external_id=doc['external_id'],
                            session_num=doc['session_num'],
                            press_num=press
                        ))
            pg_conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            raise error
        finally:
            if pg_conn is not None:
                pg_conn.close()

    def load_interpellations(self):
        """Load Interpellations"""
        find_query = {'type': self.data_type}
        if self.mongo_outcol.count_documents(find_query) == 0:
            return None

        docs = self.mongo_outcol.find(find_query)
        pg_conn = None
        try:
            pg_conn = psycopg2.connect(self.postgres_url)
            pg_cursor = pg_conn.cursor()
            query = """
                INSERT INTO parliament_interpellation (external_id, period_id, "date",
                                                        asked_by_id, status, responded_by,
                                                        recipients, url, description,
                                                        interpellation_session_id,
                                                        response_session_id,
                                                        press_id)
                VALUES (
                    {external_id},
                    (
                        SELECT id FROM parliament_period WHERE period_num = {period_num}
                    ),
                    '{date}',
                    (
                        SELECT M.id FROM parliament_member M
                        INNER JOIN person_person P ON M.person_id = P.id
                        INNER JOIN parliament_period E ON M.period_id = E.id
                        WHERE E.period_num = {period_num}
                        AND P.forename = '{asked_by_forename}'
                        AND P.surname = '{asked_by_surname}'
                    ),
                    {status},
                    '{responded_by}',
                    ARRAY[{recipients}],
                    '{url}',
                    '{description}',
                    {interpellation_session},
                    {response_session},
                    {press}
                ) ON CONFLICT DO NOTHING
            """
            for doc in docs:
                doc['recipients'] = "'{0}'".format("', '".join(doc['recipients']))
                if not 'responded_by' in doc:
                    doc['responded_by'] = ''
                if 'interpellation_session_num' in doc:
                    doc['interpellation_session'] = """
                    (
                        SELECT S.id FROM parliament_session S
                        INNER JOIN parliament_period P ON S.period_id = P.id
                        WHERE P.period_num = {period_num}
                        AND S.session_num = {interpellation_session_num}
                    )
                    """.format(**doc)
                else:
                    doc['interpellation_session'] = 'NULL'
                
                if 'response_session_num' in doc:
                    doc['response_session'] = """
                    (
                        SELECT S.id FROM parliament_session S
                        INNER JOIN parliament_period P ON S.period_id = P.id
                        WHERE P.period_num = {period_num}
                        AND S.session_num = {response_session_num}
                    )
                    """.format(**doc)
                else:
                    doc['response_session'] = 'NULL'
                if 'press_num' in doc:
                    doc['press'] = """
                    (
                        SELECT P.id FROM parliament_press P
                        INNER JOIN parliament_period E ON P.period_id = E.id
                        WHERE E.period_num = {period_num}
                        AND P.press_num = '{press_num}'
                    )
                    """.format(**doc)
                else:
                    doc['press'] = 'NULL'
                pg_cursor.execute(query.format(**doc))
            pg_conn.commit()
        
        except (Exception, psycopg2.DatabaseError) as error:
            raise error
        finally:
            if pg_conn is not None:
                pg_conn.close()

    def load_amendments(self):
        """Load Amendments"""
        find_query = {'type': self.data_type}
        if self.mongo_outcol.count_documents(find_query) == 0:
            return None

        docs = self.mongo_outcol.find(find_query)
        pg_conn = None
        try:
            pg_conn = psycopg2.connect(self.postgres_url)
            pg_cursor = pg_conn.cursor()
            main_query = """
                INSERT INTO parliament_amendment (external_id, session_id, press_id, "date",
                                                  submitter_id, voting_id, url, title)
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
                        INNER JOIN parliament_period E ON P.period_id = E.id
                        WHERE P.press_num = '{press_num}'
                        AND E.period_num = {period_num}
                    ),
                    '{date}',
                    (
                        SELECT M.id FROM parliament_member M
                        INNER JOIN parliament_period E ON M.period_id = E.id
                        INNER JOIN person_person P ON M.person_id = P.id
                        WHERE E.period_num = {period_num}
                        AND P.forename = '{submitter_forename}'
                        AND P.surname = '{submitter_surname}'
                    ),
                    {voting},
                    '{url}',
                    '{title}'

                ) ON CONFLICT DO NOTHING
            """

            related_query = """
                INSERT INTO parliament_amendment{table_suffix} (amendment_id, member_id)
                VALUES (
                    (
                        SELECT id FROM parliament_amendment WHERE external_id = {external_id}
                    ),
                    (
                        SELECT M.id FROM parliament_member M
                        INNER JOIN parliament_period E ON M.period_id = E.id
                        INNER JOIN person_person P ON M.person_id = P.id
                        WHERE E.period_num = {period_num}
                        AND P.forename = '{forename}'
                        AND P.surname = '{surname}'
                    )
                ) ON CONFLICT DO NOTHING
            """

            for doc in docs:
                doc['submitter_forename'], doc['submitter_surname'] = doc['submitter']

                if 'voting_external_id' in doc:
                    doc['voting'] = """
                    (
                        SELECT id FROM parliament_voting WHERE external_id = {}
                    )
                    """.format(doc['voting_external_id'])
                else:
                    doc['voting'] = 'NULL'
                pg_cursor.execute(main_query.format(**doc))

                if 'other_submitters' in doc:
                    for submitter in doc['other_submitters']:
                        pg_cursor.execute(related_query.format(
                            table_suffix='submitter',
                            external_id=doc['external_id'],
                            forename=submitter[0],
                            surname=submitter[1],
                            period_num=doc['period_num']
                        ))
                
                if 'signed_members' in doc:
                    for member in doc['signed_members']:
                        pg_cursor.execute(related_query.format(
                            table_suffix='signedmember',
                            external_id=doc['external_id'],
                            forename=member[0],
                            surname=member[1],
                            period_num=doc['period_num']
                        ))
            
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
        elif self.data_type == 'member_change':
            self.load_member_changes()
        elif self.data_type == 'press':
            self.load_presses()
        elif self.data_type == 'session':
            self.load_sessions()
        elif self.data_type == 'daily_club':
            self.load_club_members()
        elif self.data_type == 'voting':
            self.load_votings()
        elif self.data_type == 'bill':
            self.load_bills()
        elif self.data_type == 'debate_appearance':
            self.load_debate_appearances()
        elif self.data_type == 'interpellation':
            self.load_interpellations()
        elif self.data_type == 'amendment':
            self.load_amendments()
        else:
            raise Exception("unknown data_type {}".format(self.data_type))

class NRSRLoadPlugin(AirflowPlugin):

    name = 'nrsr_load_plugin'
    operators = [NRSRLoadOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
