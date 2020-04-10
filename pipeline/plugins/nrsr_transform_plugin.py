"""
Data Transform Plugin
"""

import _pickle as cPickle
from datetime import datetime, timedelta
from difflib import SequenceMatcher
import logging
import os
import re

from bson.objectid import ObjectId
import psycopg2
from pymongo import MongoClient

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin


class NRSRTransformOperator(BaseOperator):
    """
    Data Transform
    """

    def __init__(self, data_type, period, daily,
                 postgres_url, mongo_settings, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.data_type = data_type
        self.period = period
        self.daily = daily

        mongo_client = MongoClient(mongo_settings['uri'])
        mongo_db = mongo_client[mongo_settings['db']]
        self.mongo_col = mongo_db[mongo_settings['col']]
        self.mongo_outcol = mongo_db[mongo_settings['outcol']]
        self.postgres_url = postgres_url

    def _fields_to_dict(self, fields):
        """
        Convert list of fields to mongodb field dict
        """
        return {x: 1 for x in fields}

    def _insert_documents(self, documents, remove=[]):
        """
        Insert into MongoDB
        """

        if remove:
            self.mongo_outcol.remove({'type': {'$in': remove}})

        self.mongo_outcol.insert_many(documents)

    def _get_documents(self, fields_dict, *aggregation, unwind=None, projection=None):
        """
        Get MongoDB Cursor
        """

        filter_dict = {'type': self.data_type}
        if self.period:
            filter_dict['period_num'] = self.period
        if self.daily and self.data_type not in ['daily_club']:
            now = datetime.utcnow() - timedelta(days=30)
            filter_dict['_id'] = {'$gte': ObjectId.from_datetime(now)}

        if unwind and projection:
            docs = self.mongo_col.aggregate([
                {'$match': filter_dict},
                unwind,
                projection
            ])
        elif aggregation:
            aggregation = [{'$match': filter_dict}] + list(aggregation)
            docs = self.mongo_col.aggregate(aggregation)
        else:
            docs = self.mongo_col.find(filter_dict, fields_dict)
        return docs

    def _copy_doc(self, document):
        return cPickle.loads(cPickle.dumps(document, -1))

    def _get_wanted_keys(self, document, keys):
        """Reduce dictionary to wanted keys only"""
        return {k: v for k, v in document.items() if k in keys}

    def transform_members(self):
        """
        Transform MPs
        """

        fields_list = [
            'external_id',
            'forename',
            'surname',
            # 'title',
            'stood_for_party',
            # 'born',
            # 'nationality',
            # 'residence',
            # 'county',
            # 'email',
            'image_urls',
            'period_num',
            'url',
            'memberships',
            'type'
        ]
        fields_dict = self._fields_to_dict(fields_list)

        new_docs = []

        residence_replacements = {
            'Tvrdošín-Medvedzie': 'Medvedzie',
            'Kostolná-Záriečie': 'Kostolná - Záriečie',
            'Ivánka pri Dunaji': 'Ivanka pri Dunaji',
            'Nový Život - Eliášovce': 'Eliášovce',
            'Michalovce - Čečehov': 'Čečehov',
            'Hnúšťa - Likier': 'Likier'
        }
        county_replacements = {
            'Trenčín': 'Trenčiansky',
            'Nitra': 'Nitriansky',
        }
        nationality_replacements = {
            'slovenská': 'slovak',
            'maďarská': 'hungarian',
            'rómska': 'romani',
            'rusínska': 'rusyn',
            'ruská': 'russian',
            'česká': 'czech',
            'ukrajinská': 'ukrainian',
            '': 'unknown'
        }

        # pg_conn = psycopg2.connect(self.postgres_url)
        # pg_cursor = pg_conn.cursor()

        # wanted_keys = [
        #     'external_id', 'period_num', 'forename', 'surname', 'title',
        #     'email', 'born', 'nationality',
        #     'external_photo_url', 'stood_for_party', 'url', 'type'
        # ]
        wanted_keys = [
            'external_id', 'external_photo_url', 'period_num',
            'forename', 'surname', 'stood_for_party', 'url', 'type'
        ]

        for doc in self._get_documents(fields_dict):
            new_doc = self._copy_doc(doc)
            for key, val in new_doc.items():
                if isinstance(val, str):
                    new_doc[key] = val.strip()
            new_doc['external_photo_url'] = new_doc['image_urls'][0]
            # GDPR
            # if new_doc['residence'] in residence_replacements:
            #     new_doc['residence'] = residence_replacements[new_doc['residence']]

            # if new_doc['external_id'] == 184:
            #     new_doc['residence'] = 'Bratislava'

            # if new_doc['residence'] == '':
            #     new_doc['residence'] = 'Neuvedené'

            # if new_doc['county'] in county_replacements:
            #     new_doc['county'] = county_replacements[new_doc['county']]

            # if new_doc['nationality'] in nationality_replacements:
            #     new_doc['nationality'] = nationality_replacements[new_doc['nationality']]

            # region = new_doc['county'] if 'kraj' in new_doc['county'] else '{} kraj'.format(new_doc['county'])
            # residence_query = """
            #     SELECT V.id, V.full_name, D.name, R.name
            #     FROM geo_village V
            #     INNER JOIN geo_district D ON V.district_id = D.id
            #     INNER JOIN geo_region R ON (D.region_id = R.id)
            #     WHERE V.full_name = '{village}' AND R.name = '{region}'
            #     """.format(village=new_doc['residence'], region=region)
            # pg_cursor.execute(residence_query)
            # villages = pg_cursor.fetchall()
            # residence_id = None
            # if not villages:
            #     raise Exception("Residence can't be paired for {}".format(new_doc))

            # elif len(villages) > 1 and new_doc['external_id'] == 708:
            #     for village in villages:
            #         if village[2] == 'Žarnovica':
            #             residence_id = village[0]
            #     if not residence_id:
            #         raise Exception(
            #             "Residence doesn't match on {} for {}".format(new_doc, villages))
            # else:
            #     residence_id = villages[0][0]

            # if not residence_id:
            #     raise Exception("residence_id for {} is None".format(new_doc))

            new_doc = self._get_wanted_keys(new_doc, wanted_keys)
            # new_doc['residence_id'] = residence_id
            new_docs.append(new_doc)
        if new_docs:
            self._insert_documents(new_docs, remove=[self.data_type])

    def transform_member_changes(self):
        """
        Transform MP changes
        """
        fields_list = [
            'external_id',
            'date',
            'period_num',
            'change_type',
            'change_reason',
            'type'
        ]
        fields_dict = self._fields_to_dict(fields_list)

        new_docs = []
        change_type = {
            "Mandát sa neuplatňuje": 0,
            "Mandát vykonávaný (aktívny poslanec)": 1,
            "Mandát náhradníka zaniknutý": 2,
            "Mandát náhradníka vykonávaný": 3,
            "Mandát náhradníka získaný": 4,
            "Mandát zaniknutý": 5,
            "Mandát nadobudnutý vo voľbách": 6
        }

        for doc in self._get_documents(fields_dict):
            new_doc = self._copy_doc(doc)
            new_doc['change_type'] = change_type[doc['change_type']]
            del new_doc['_id']
            new_docs.append(new_doc)

        if new_docs:
            self._insert_documents(new_docs, remove=[self.data_type])



    def transform_presses(self):
        """
        Transform Press data
        """
        fields_list = [
            'title',
            'press_num',
            'group_num',
            'period_num',
            'press_type',
            'date',
            'attachments_urls',
            'attachments_names',
            'url',
            'type'
        ]

        fields_dict = self._fields_to_dict(fields_list)

        press_type_replacements = {
            'Návrh zákona': 1,
            'Iný typ': 2,
            'Informácia': 7,
            'Správa': 5,
            'Petícia': 3,
            'Medzinárodná zmluva': 4,

        }

        wanted_keys = ['press_type', 'title', 'press_num', 'date', 'period_num', 'url', 'type']
        new_docs = []
        for doc in self._get_documents(fields_dict):
            new_doc = self._copy_doc(doc)
            new_doc['press_type'] = press_type_replacements[new_doc['press_type']]
            new_doc = self._get_wanted_keys(new_doc, wanted_keys)
            new_docs.append(new_doc)

        if new_docs:
            self._insert_documents(new_docs, remove=[self.data_type])

    def transform_sessions(self):
        """
        Transform Session data
        """

        fields_list = ['url', 'period_num', 'external_id', 'name', 'program_points', 'type']
        fields_dict = self._fields_to_dict(fields_list)

        state_replacements = {
            'Prerokovaný bod programu': 0,
            'Neprerokovaný bod programu': 1,
            'Presunutý bod programu': 2,
            'Stiahnutý bod programu': 3,
            'Prerušené rokovanie o bode programu': 4,
        }

        wanted_keys = ['external_id', 'period_num', 'name', 'type', 'url', 'program_points', 'session_num']
        new_docs = []
        for doc in self._get_documents(fields_dict):
            new_doc = self._copy_doc(doc)
            try:
                new_doc['session_num'] = int(''.join(re.findall(r'\d+', new_doc['name'][:15])))
            except ValueError:
                new_doc['session_num'] = None

            program_points = []
            for point in new_doc['program_points']:
                program_points.append({
                    'state': state_replacements[point['state']],
                    'point_num': point['progpoint'],
                    'press_num': point['parlpress'],
                    'text1': point['text'][0],
                    'text2': point['text'][1],
                    'text3': point['text'][2],
                })
            new_doc['program_points'] = program_points
            new_doc = self._get_wanted_keys(new_doc, wanted_keys)
            new_docs.append(new_doc)

        if new_docs:
            self._insert_documents(new_docs, remove=[self.data_type])

    def transform_club_members(self):
        """
        Transform club members
        """

        max_voting = list(
            self.mongo_col.find({
                'type': 'voting'
            }, {
                'datetime': 1
            }).sort([('datetime', -1)]).limit(1))[0]['datetime'].replace(
                hour=12, minute=0, second=0, microsecond=0)

        # TODO(Jozef): the if/else condition won't work on last day of period
        aggregation = [
            {
                '$unwind': '$clubs'
            },
            {
                '$project': {
                    'period_num': 1,
                    'date': 1,
                    'club_name': {
                        '$arrayElemAt': ['$clubs', 0]
                    },
                    'club_values': {
                        '$arrayElemAt': ['$clubs', 1]
                    }
                }
            }, {
                '$unwind': '$club_values'
            }, {
                '$sort': {
                    'club_values': 1,
                    'date': 1
                }
            },
            {
                '$group': {
                    '_id': {
                        'period_num': '$period_num',
                        'member_external_id': '$club_values',
                        'club': '$club_name',
                        'date': '$date',
                    },
                }
            },
            {
                '$project': {
                    '_id': '$_id',
                    'period_num': '$_id.period_num',
                    'member_external_id': '$_id.member_external_id',
                    'date': '$_id.date',
                    'club': '$_id.club',
                }
            },
            {'$sort': {'_id.period_num': 1, '_id.member_external_id': 1, '_id.date': 1}}
        ]
        member_sequence = {}
        docs = list(self._get_documents({}, *aggregation))
        docs_len = len(docs)

        pg_conn = psycopg2.connect(self.postgres_url)
        pg_cursor = pg_conn.cursor()

        active_query = """
        SELECT 1 FROM person_person PP
            INNER JOIN parliament_member PM ON PM.person_id = PP.id
            INNER JOIN parliament_memberactive PMA ON PMA.member_id = PM.id
        WHERE PMA.start <= '{date}' AND (PMA.end > '{date}' OR PMA.end IS NULL)
        AND PP.external_id = {external_id}
        LIMIT 1
        """

        for index, doc in enumerate(docs):
            _id = doc['_id']
            period_num = _id['period_num']
            club_key = _id['club']
            member_key = _id['member_external_id']

            if period_num not in member_sequence:
                member_sequence[period_num] = {}

            if club_key not in member_sequence[period_num]:
                member_sequence[period_num][club_key] = {}

            if member_key not in member_sequence[period_num][club_key]:
                member_sequence[period_num][club_key][member_key] = []

            member_link = member_sequence[period_num][club_key][member_key]

            if index < docs_len - 1:
                next_item = docs[index + 1]['_id']
            else:
                next_item = None

            if member_link:
                # last was ended, create new
                if member_link[-1][2]:
                    member_link.append([_id['date'], None, False])
            else:
                member_link.append([_id['date'], None, False])

            current = member_link[-1]

            if next_item:
                if next_item['member_external_id'] == member_key and next_item['club'] != club_key:
                    if _id['date'] == max_voting:
                        current[1] = None
                    else:
                        current[1] = _id['date']
                    current[2] = True
                elif next_item['member_external_id'] != member_key:
                    pg_cursor.execute(active_query.format(date=max_voting, external_id=member_key))
                    db_result = pg_cursor.fetchall()
                    if not db_result:
                        current[1] = _id['date']
                        current[2] = True

        if pg_conn is not None:
            pg_conn.close()

        new_docs = []
        for period_num, clubs in member_sequence.items():
            for club, members in clubs.items():
                for member, changes in members.items():
                    for change in changes:
                        new_docs.append({
                            'type': self.data_type,
                            'period_num': period_num,
                            'member_external_id': member,
                            'club': club,
                            'start': change[0],
                            'end': change[1],
                        })

        if new_docs:
            self._insert_documents(new_docs, remove=[self.data_type])

    def transform_votings(self):
        """
        Transform votings and votes
        """

        fields_list = [
            'external_id', 'topic', 'datetime', 'session_num', 'voting_num',
            'result', 'period_num', 'press_num', 'url', 'votes', 'type'
        ]
        fields_dict = self._fields_to_dict(fields_list)

        result_replacements = {
            "Návrh prešiel": 0,
            "Návrh neprešiel": 1,
            "Parlament nebol uznášaniaschopný": 2
        }

        vote_replacements = {
            "-": -1,
            "Z": 0,
            "P": 1,
            "?": 2,
            "N": 3,
            "0": 4
        }

        new_docs = []
        for doc in self._get_documents(fields_dict):
            new_doc = self._copy_doc(doc)
            new_doc['result'] = result_replacements[new_doc['result']]
            for vote in new_doc['votes']:
                vote['vote'] = vote_replacements[vote['vote']]
            del new_doc['_id']
            new_docs.append(new_doc)

        if new_docs:
            self._insert_documents(new_docs, remove=[self.data_type])


    def transform_bills(self):
        """
        Transform bills
        """
        fields_list = [
            'type',
            'external_id',
            'period_num',
            'press_num',
            'proposer',
            'delivered',
            'current_state',
            'current_result',
            'category_name',
            'url',
            'type',
        ]

        fields_dict = self._fields_to_dict(fields_list)

        current_state_replacements = {
            "Evidencia": 0,
            "Uzavretá úloha": 1,
            "Rokovanie NR SR": 8,
            "Rokovanie gestorského výboru": 9,
            "I. čítanie": 2,
            "II. čítanie": 3,
            "III. čítanie": 4,
            "Redakcia": 5,
            "Výber poradcov k NZ": 10,
            "Stanovisko k NZ": 7
        }

        current_result_replacements = {
            "(NR SR nebude pokračovať v rokovaní o návrhu zákona)": 0,
            "(NZ vzal navrhovateľ späť)": 1,
            "(Zákon vyšiel v Zbierke zákonov)": 7,
            "(NZ nebol schválený)": 5,
            "(Zákon bol vrátený prezidentom)": 8,
            "(Zápis spoločnej správy výborov)": 4,
            "(Zapísané uznesenie výboru)": 9,
            "(NZ postúpil do II. čítania)": 11,
            "(NZ postúpil do redakcie)": 3,
            "(Výber právneho poradcu)": 10,
            "(Pripravená informácia k NZ)": 6
        }

        category_name_replacements = {
            "Novela zákona": 0,
            "Návrh nového zákona": 1,
            "Iný typ": 2,
            "Petícia": 3,
            "Medzinárodná zmluva": 4,
            "Správa": 5,
            "Ústavný zákon": 6,
            "Informácia": 7,
            "Návrh zákona o štátnom rozpočte": 8,
            "Zákon vrátený prezidentom": 9,

        }

        new_docs = []
        for doc in self._get_documents(fields_dict):
            if 'category_name' not in doc:
                # empty bill with no details
                continue
            press_title = ''
            press_num = ''
            if 'press_num' in doc:
                presses = list(self.mongo_col.find({
                    'type': 'press', 'press_num': doc['press_num'], 'period_num': doc['period_num']}))
                if not presses:
                    raise Exception("Missing press for {}".format(doc))
                if len(presses) > 1:
                    raise Exception("Multiple presses for {}".format(doc))
                else:
                    press_title = presses[0]['title']
                    press_num = presses[0]['press_num']


            doc['press_num'] = press_num
            names = []
            proposers = []
            doc['proposer_nonmember'] = ''
            proposer_type = 'NULL'
            if 'proposer' in doc:
                if doc['proposer'].startswith('poslanci NR SR'):
                    doc['proposer_nonmember'] = 'poslanci NR SR'
                    proposer_type = 0
                    try:
                        names = re.match(r'^.*\(([^)]+)\)$', doc['proposer']).groups()[0].split(', ')
                    except AttributeError:
                        names = []
                elif doc['proposer'].startswith('vláda'):
                    proposer_type = 1
                    propre = re.match(r'vláda \((.*)\)', doc['proposer'])
                    if propre and len(propre.groups()) == 1:
                        doc['proposer_nonmember'] = propre.groups()[0]
                    else:
                        doc['proposer_nonmember'] = doc['proposer']
                elif doc['proposer'].startswith('výbor'):
                    proposer_type = 2
                    doc['proposer_nonmember'] = doc['proposer']
                query = """
                    SELECT M.id, S.forename, S.surname FROM parliament_member M
                    INNER JOIN parliament_period P ON M.period_id = P.id
                    INNER JOIN person_person S ON M.person_id = S.id
                    WHERE P.period_num = {period_num}
                    AND S.surname = '{surname}' AND S.forename LIKE '{forename}%'
                    """
                pg_conn = psycopg2.connect(self.postgres_url)
                pg_cursor = pg_conn.cursor()
                for name in names:
                    name_list = name.split('.\xa0')
                    pg_cursor.execute(
                        query.format(
                            period_num=doc['period_num'],
                            forename=name_list[0],
                            surname=name_list[1]
                        )
                    )
                    rows = pg_cursor.fetchall()
                    if len(rows) == 1:
                        proposers.append(rows[0][0])
                    elif len(rows) == 0:
                        raise Exception("Missing member in {}".format(query))
                    else:
                        try:
                            similarities = []
                            for row in rows:
                                similarities.append(
                                    SequenceMatcher(
                                        None, '{} {}'.format(row[1], row[2]),
                                        press_title).ratio())
                            max_ratio = max(similarities)
                            proposers.append(rows[similarities.index(max_ratio)][0])
                        except Exception as exc:
                            raise Exception(
                                "Similarities search failed on {} with {}".format(doc, exc))
                pg_conn.close()
                doc['proposers'] = proposers

            new_doc = self._copy_doc(doc)
            new_doc['proposer_type'] = proposer_type
            try:
                new_doc['current_state'] = current_state_replacements[new_doc['current_state']]
            except KeyError:
                new_doc['current_state'] = None
            try:
                new_doc['current_result'] = current_result_replacements[new_doc['current_result']]
            except KeyError:
                new_doc['current_result'] = None
            new_doc['category_name'] = category_name_replacements[new_doc['category_name']]

            new_doc = self._get_wanted_keys(new_doc, [
                'type', 'period_num', 'press_num', 'external_id', 'delivered',
                'current_state', 'current_result', 'proposers',
                'proposer_nonmember', 'proposer_type', 'category_name', 'url'
            ])
            new_docs.append(new_doc)


        if new_docs:
            self._insert_documents(new_docs, remove=[self.data_type])

    # def transform_bill_process_steps(self):
    #     """
    #     Transform bill process steps
    #     """
    #     fields_list = [
    #         'type',

    #         'period_num',

    #         'external_id',
    #         'bill_id',
    #         'body_label',
    #         'meeting_session_num',
    #         'meeting_resolution',
    #         'step_result',

    #         'committees_label',
    #         'slk_label',
    #         'coordinator_label',
    #         'coordinator_meeting_date',??
    #         'coordinator_name',??
    #         'discussed_label',
    #         'sent_standpoint',
    #         'sent_label',
    #         'act_num_label'


    #         'url'
    #     ]


    def transform_debate_appearances(self):
        """
        Debate Appearances
        """
        fields_list = [
            'type',
            'external_id',
            'period_num',
            'debater_name',
            'debater_party',
            'debater_role',
            'start',
            'end',
            'session_num',
            'press_num',
            'appearance_type',
            'appearance_type_addition',
            'video_short_url',
            'text',
        ]

        fields_dict = self._fields_to_dict(fields_list)

        appearance_type_replacements = {
            "-": 0,
            "Doplňujúca otázka / reakcia zadávajúceho": 1,
            "Prednesenie interpelácie": 2,
            "Prednesenie otázky": 3,
            "Uvádzajúci uvádza bod": 4,
            "Vstup predsedajúceho": 5,
            "Vystúpenie": 6,
            "Vystúpenie s faktickou poznámkou": 7,
            "Vystúpenie s procedurálnym návrhom": 8,
            "Vystúpenie spoločného spravodajcu": 9,
            "Vystúpenie v rozprave": 10,
            "Zodpovedanie otázky": 11,
        }

        new_docs = []
        for doc in self._get_documents(fields_dict):
            doc['appearance_type'] = appearance_type_replacements[doc['appearance_type']]
            name_list = doc['debater_name'].split(', ')
            doc['debater_forename'] = name_list[1]
            doc['debater_surname'] = name_list[0]
            if not 'text' in doc:
                doc['text'] = ''
            else:
                doc['text'] = ''.join(doc['text'])
            if 'debater_party' not in doc:
                doc['debater_party'] = None
            try:
                party = re.match(r'^\(+(.+)+\)', doc['debater_party'])
                if party:
                    doc['debater_party'] = party.groups()[0]
            except TypeError:
                pass

            if 'debater_role' not in doc:
                doc['debater_role'] = ''
            doc['debater_role'] = doc['debater_role'][2:]

            if 'NRSR' in doc['debater_role'] or 'NR SR' in doc['debater_role']:
                doc['parliament_member'] = True
            else:
                doc['parliament_member'] = False

            new_docs.append(
                self._get_wanted_keys(doc, [
                    'type', 'period_num', 'session_num', 'press_num', 'external_id',
                    'start', 'end', 'debater_forename', 'debater_surname', 'debater_role',
                    'parliament_member', 'debater_party', 'appearance_type',
                    'appearance_type_addition', 'video_short_url', 'text'
                ])
            )

        if new_docs:
            self._insert_documents(new_docs, remove=[self.data_type])

    def transform_interpellations(self):
        """
        Transform Interpellations
        """
        fields_list = [
            'type',
            'external_id',
            'period_num',
            'status',
            'asked_by',
            'description',
            'recipients',
            'date',
            'responded_by',
            'interpellation_session_num',
            'response_session_num',
            'responded_by',
            'press_num',
            'url'
        ]
        fields_dict = self._fields_to_dict(fields_list)

        status_replacements = {
            "Príjem odpovede na interpeláciu": 0,
            "Rokovanie o interpelácii": 1,
            "Uzavretá odpoveď na interpeláciu": 2,
            "Interpelácia na expedíciu": 3,
        }
        new_docs = []
        for doc in self._get_documents(fields_dict):
            doc['status'] = status_replacements[doc['status']]
            asked_list = doc['asked_by'].split(', ')
            doc['asked_by_forename'] = asked_list[1]
            doc['asked_by_surname'] = asked_list[0]

            new_docs.append(
                self._get_wanted_keys(doc, [
                    'type', 'external_id', 'period_num', 'status',
                    'asked_by_forename', 'asked_by_surname', 'description',
                    'recipients', 'date', 'responded_by', 'interpellation_session_num',
                    'response_session_num', 'responded_by', 'press_num', 'url'
                ])
            )

        if new_docs:
            self._insert_documents(new_docs, remove=[self.data_type])

    def transform_amendments(self):
        """Transform amendments"""
        fields_list = [
            'type',
            'external_id',
            'period_num',
            'press_num',
            'session_num',
            'submitter',
            'other_submitters',
            'date',
            'signed_members',
            'voting_external_id',
            'url'
        ]

        fields_dict = self._fields_to_dict(fields_list)
        new_docs = []
        for doc in self._get_documents(fields_dict):
            doc['submitter'] = doc['submitter'].split(', ')[::-1]

            if 'other_submitters' in doc:
                doc['other_submitters'] = [x.split(', ')[::-1] for x in doc['other_submitters']]

            if 'signed_members' in doc:
                doc['signed_members'] = [x.split(', ')[::-1] for x in doc['signed_members']]
            new_docs.append(self._get_wanted_keys(doc, fields_list))

        if new_docs:
            self._insert_documents(new_docs, remove=[self.data_type])

    def transform_committees(self):
        """Committees and memberships"""
        fields_list = [
            '_id',
            'type',
            'period_num',
            'external_id',
            'name',
            'description',
            'url',
            'members'
        ]

        roles = {
            'predsedníčka': 0,
            'predseda': 1,
            'podpredsedníčka': 2,
            'podpredsedkyňa': 2, # incorrect phrase promoted to nrsr.sk
            'podpredseda': 3,
            'členka': 4,
            'člen': 5,
            'náhradná členka': 6,
            'náhradný člen': 7,
            'overovateľka': 8,
            'overovateľ': 9
        }

        fields_dict = self._fields_to_dict(fields_list)
        new_docs = []
        for doc in self._get_documents(fields_dict):
            doc['description'] = ''.join([x.strip() for x in doc['description']])
            for member in doc['members']:
                member['role'] = roles[member['role']]
                member['start'] = doc['_id'].generation_time
            new_docs.append(self._get_wanted_keys(doc, fields_list))
        if new_docs:
            self._insert_documents(new_docs, remove=[self.data_type])

    def transform_committee_schedules(self):
        """Transform committee sessions and session points"""
        fields_list = [
            'type',
            'committee_name',
            'period_num',
            'date',
            'time',
            'place',
            'points'
        ]
        fields_dict = self._fields_to_dict(fields_list)
        new_docs = []
        pg_conn = psycopg2.connect(self.postgres_url)
        pg_cursor = pg_conn.cursor()
        for doc in self._get_documents(fields_dict):
            times = doc['time'].split(' - ')
            if len(times) == 1:
                time_start = times[0]
                time_end = None
            else:
                time_start, time_end = times

            pg_cursor.execute(
                """
                SELECT PC.id FROM parliament_committee PC
                LEFT JOIN parliament_period PP ON PP.id = PC.period_id
                WHERE PC.name = '{committee_name}' AND PP.period_num = {period_num}
                """.format(
                    committee_name=doc['committee_name'],
                    period_num=doc['period_num']
                )
            )
            row = pg_cursor.fetchone()

            try:
                dt_start = datetime.strptime('{} {}'.format(doc['date'], time_start),
                                  '%d. %m. %Y %H:%M')
            except ValueError:
                dt_start = datetime.strptime('{} {}'.format(doc['date'], time_start),
                                  '%d.%m.%Y %H:%M')

            try:
                dt_end = datetime.strptime('{} {}'.format(doc['date'], time_end),
                                  '%d. %m. %Y %H:%M') if time_end else None
            except ValueError:
                dt_end = datetime.strptime('{} {}'.format(doc['date'], time_end),
                                  '%d.%m.%Y %H:%M') if time_end else None

            new_doc = {
                'type': doc['type'],
                'period_num': doc['period_num'],
                'place':
                ''.join([x.strip() for x in doc['place']]),
                'start': dt_start,
                'end': dt_end,
                'committee_id':
                row[0]
            }
            points = []
            pg_cursor.execute(
                """
                SELECT PRESS.id, PRESS.press_num FROM parliament_press PRESS
                LEFT JOIN parliament_period PER ON PER.id = PRESS.period_id
                WHERE PER.period_num = {period_num}
                """.format(period_num=doc['period_num'])
            )
            presses = {x[1]: x[0] for x in pg_cursor.fetchall()}
            for point in doc['points']:
                index = None
                text = ''.join([x.strip() for x in point['text']])
                if not text:
                    continue
                is_index = re.match(r'^([0-9]\. )([\s\S]*)$', text)
                if is_index:
                    index = is_index.groups()[0].strip().replace('.', '')
                    text = ''.join(is_index.groups()[1:])

                press_id = None
                if point['press_num']:
                    press_id = presses[str(point['press_num'])]
                points.append({
                    'press_id': press_id,
                    'index': index,
                    'topic': text
                })
            new_doc['points'] = points
            new_docs.append(new_doc)

        if new_docs:
            self._insert_documents(new_docs, remove=[self.data_type])

        if pg_conn is not None:
            pg_conn.close()

    def execute(self, context):
        """Operator Executor"""
        if self.data_type == 'member':
            self.transform_members()
        elif self.data_type == 'member_change':
            self.transform_member_changes()
        elif self.data_type == 'press':
            self.transform_presses()
        elif self.data_type == 'session':
            self.transform_sessions()
        # elif self.data_type == 'club':
        #     data_frame = self.transform_clubs()
        elif self.data_type == 'daily_club':
            self.transform_club_members()
        elif self.data_type == 'voting':
            self.transform_votings()
        elif self.data_type == 'bill':
            self.transform_bills()
        # elif self.data_type == 'bill_step':
        #     self.transform_bill_process_steps()
        elif self.data_type == 'debate_appearance':
            self.transform_debate_appearances()
        elif self.data_type == 'interpellation':
            self.transform_interpellations()
        elif self.data_type == 'amendment':
            self.transform_amendments()
        elif self.data_type == 'committee':
            self.transform_committees()
        elif self.data_type == 'committeeschedule':
            self.transform_committee_schedules()


class NRSRTransformPlugin(AirflowPlugin):

    name = 'nrsr_transform_plugin'
    operators = [NRSRTransformOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
