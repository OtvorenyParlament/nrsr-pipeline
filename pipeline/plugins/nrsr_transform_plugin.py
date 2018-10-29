"""
Data Transform Plugin
"""

from datetime import datetime, timedelta
import logging
import os
import re

from bson.objectid import ObjectId
import pandas
import psycopg2
from pymongo import MongoClient

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin


class NRSRTransformOperator(BaseOperator):
    """
    Data Transform
    """

    def __init__(self, data_type, period, daily, postgres_url, mongo_settings, file_dest, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.data_type = data_type
        self.period = period
        self.daily = daily
        self.file_dest = file_dest

        mongo_client = MongoClient(mongo_settings['uri'])
        mongo_db = mongo_client[mongo_settings['db']]
        self.mongo_col = mongo_db[mongo_settings['col']]
        self.postgres_url = postgres_url

    def _get_documents(self, fields_dict, *aggregation, unwind=None, projection=None):
        """
        Get MongoDB Cursor
        """

        filter_dict = {'type': self.data_type}
        if self.period:
            filter_dict['period_num'] = str(self.period)
        if self.daily:
            now = datetime.utcnow() - timedelta(hours=36)
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



    def transform_members(self):
        """
        Transform MPs
        """

        fields_list = [
            'external_id',
            'forename',
            'surname',
            'title',
            'stood_for_party',
            'born',
            'nationality',
            'residence',
            'county',
            'email',
            'image_urls',
            'period_num',
            'url',
            'memberships'
        ]
        fields_dict = {x: 1 for x in fields_list}

        # TODO(Jozef): Monitor memory consumption of this
        docs = list(self._get_documents(fields_dict))
        member_frame = pandas.DataFrame(docs)
        if member_frame.empty:
            return member_frame

        pg_conn = psycopg2.connect(self.postgres_url)
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(
            """
            SELECT "geo_village"."id", "geo_village"."full_name", "geo_district"."name", "geo_region"."name" 
            FROM "geo_village" 
            INNER JOIN "geo_district" ON ("geo_village"."district_id" = "geo_district"."id") 
            INNER JOIN "geo_region" ON ("geo_district"."region_id" = "geo_region"."id")
            """
        )
        villages = pg_cursor.fetchall()
        if not villages:
            raise Exception("No Villages are imported")

        village_frame = pandas.DataFrame(
            villages, columns=['id', 'village_name', 'district_name', 'region_name'])


        # strip everything
        member_frame = member_frame.applymap(lambda x: x.strip() if type(x) is str else x)

        # external photo url
        member_frame['external_photo_url'] = member_frame['image_urls'].map(lambda x: x[0])

        # fix villages
        member_frame.residence.replace(['Tvrdošín-Medvedzie'], 'Medvedzie', inplace=True)
        member_frame.residence.replace(
            ['Kostolná-Záriečie'], 'Kostolná - Záriečie', inplace=True)
        member_frame.residence.replace(['Ivánka pri Dunaji'], 'Ivanka pri Dunaji', inplace=True)
        member_frame.residence.replace(['Nový Život - Eliášovce'], 'Eliášovce', inplace=True)
        member_frame.residence.replace(['Michalovce - Čečehov'], 'Čečehov', inplace=True)
        member_frame.residence.replace(['Hnúšťa - Likier'], 'Likier', inplace=True)
        member_frame[
            member_frame.external_id == '184'].replace(['Čaklov'], 'Bratislava', inplace=True)

        member_frame.residence.replace([''], 'Neuvedené', inplace=True)

        # convert born to date
        member_frame['born'] = pandas.to_datetime(member_frame['born'], format='%d. %m. %Y')

        # fix counties
        member_frame.county.replace(['Trenčín'], 'Trenčiansky kraj', inplace=True)
        member_frame.county.replace(['Nitra'], 'Nitriansky kraj', inplace=True)

        # fix nationalities
        member_frame.nationality.replace(['slovenská'], 'slovak', inplace=True)
        member_frame.nationality.replace(['maďarská'], 'hungarian', inplace=True)
        member_frame.nationality.replace(['rómska'], 'romani', inplace=True)
        member_frame.nationality.replace(['rusínska'], 'rusyn', inplace=True)
        member_frame.nationality.replace(['ruská'], 'russian', inplace=True)
        member_frame.nationality.replace(['česká'], 'czech', inplace=True)
        member_frame.nationality.replace(['ukrajinská'], 'ukrainian', inplace=True)
        member_frame.nationality.replace([''], 'unknown', inplace=True)

        member_frame = member_frame.merge(
            village_frame, left_on='residence', right_on='village_name', how='left')

        # fix specific village
        member_frame.drop(member_frame.loc[
            (member_frame.external_id == '708') & (member_frame.district_name == 'Žarnovica')
        ].index, inplace=True)

        # id to residence_id
        member_frame.rename({'id': 'residence_id'}, axis='columns', inplace=True)
        member_frame = member_frame[
            ['external_id', 'period_num', 'forename', 'surname', 'title', 'email',
            'born', 'nationality', 'residence_id', 'external_photo_url', 'stood_for_party', 'url']]

        return member_frame

    def transform_member_changes(self):
        """
        Transform MP changes
        """
        fields_list = [
            'external_id',
            'date',
            'period_num',
            'change_type',
            'change_reason'
        ]
        fields_dict = {x: 1 for x in fields_list}

        # TODO(Jozef): Monitor memory consumption of this
        docs = list(self._get_documents(fields_dict))
        change_frame = pandas.DataFrame(docs)
        if change_frame.empty:
            return change_frame

        change_frame.change_type.replace(
            ["Mandát sa neuplatňuje"], 'doesnotapply', inplace=True)
        change_frame.change_type.replace(
            ["Mandát vykonávaný (aktívny poslanec)"], 'active', inplace=True)
        change_frame.change_type.replace(
            ["Mandát náhradníka zaniknutý"], 'substitutefolded', inplace=True)
        change_frame.change_type.replace(
            ["Mandát náhradníka vykonávaný"], 'substituteactive', inplace=True)
        change_frame.change_type.replace(
            ["Mandát náhradníka získaný"], 'substitutegained', inplace=True)
        change_frame.change_type.replace(
            ["Mandát zaniknutý"], 'folded', inplace=True)
        change_frame.change_type.replace(
            ["Mandát nadobudnutý vo voľbách"], 'gained', inplace=True)

        change_frame['date'] = pandas.to_datetime(
            change_frame['date'], format='%d. %m. %Y')
        change_frame = change_frame[['external_id', 'date', 'period_num', 'change_type', 'change_reason']]

        return change_frame

    def transform_presses(self):
        """
        Transform Press data
        """
        fields_list = [
            'title',
            'num',
            'group_num',
            'period_num',
            'press_type',
            'date',
            'attachments_urls',
            'attachments_names',
            'url'
        ]

        fields_dict = {x: 1 for x in fields_list}

        # TODO(Jozef): Monitor memory consumption of this
        docs = list(self._get_documents(fields_dict))
        press_frame = pandas.DataFrame(docs)
        if press_frame.empty:
            return press_frame

        press_frame.press_type.replace(['Návrh zákona'], 'bill', inplace=True)
        press_frame.press_type.replace(['Iný typ'], 'other', inplace=True)
        press_frame.press_type.replace(['Informácia'], 'information', inplace=True)
        press_frame.press_type.replace(['Správa'], 'report', inplace=True)
        press_frame.press_type.replace(['Petícia'], 'petition', inplace=True)
        press_frame.press_type.replace(['Medzinárodná zmluva'], 'intag', inplace=True)
        press_frame.rename({'num': 'press_num'}, axis='columns', inplace=True)
        press_frame['date'] = pandas.to_datetime(
            press_frame['date'], format='%d. %m. %Y')

        # TODO(add attachments)
        press_frame = press_frame[[
            'press_type', 'title', 'press_num', 'date', 'period_num', 'url']]
        return press_frame


    def transform_sessions(self):
        """
        Transform Session data
        """
        fields_dict = {}

        # TODO(Jozef): Monitor memory consumption of this
        unwind = {'$unwind': '$program_points'}
        projection = {
            '$project': {
                '_id': 1,
                'url': 1,
                'period_num': 1,
                'external_id': 1,
                'name': 1,
                'state': '$program_points.state',
                'progpoint': '$program_points.progpoint',
                'parlpress': '$program_points.parlpress',
                'text1': {'$arrayElemAt': ['$program_points.text', 0]},
                'text2': {'$arrayElemAt': ['$program_points.text', 1]},
                'text3': {'$arrayElemAt': ['$program_points.text', 2]},
            }
        }
        docs = list(self._get_documents(fields_dict, unwind=unwind, projection=projection))

        session_frame = pandas.DataFrame(docs)
        if session_frame.empty:
            return session_frame

        session_frame['session_num'] = session_frame['name'].apply(
            lambda x: ''.join(re.findall(r'\d+', x[:15])))
        session_frame['progpoint'] = session_frame['progpoint'].apply(
            lambda x: x.replace('.', ''))

        session_frame.state.replace(['Prerokovaný bod programu'], 'discussed', inplace=True)
        session_frame.state.replace(['Neprerokovaný bod programu'], 'notdiscussed', inplace=True)
        session_frame.state.replace(['Presunutý bod programu'], 'moved', inplace=True)
        session_frame.state.replace(['Stiahnutý bod programu'], 'withdrawn', inplace=True)
        session_frame.state.replace(['Prerušené rokovanie o bode programu'], 'interrupted', inplace=True)
        session_frame.fillna({'text1': '', 'text2': '', 'text3': ''}, inplace=True)
        session_frame = session_frame.where(session_frame.notnull(), None)

        # TODO(Jozef): Add attachments
        session_frame = session_frame[[
            'external_id', 'session_num', 'period_num', 'name', 'state',
            'progpoint', 'parlpress', 'text1', 'text2', 'text3', 'url'
        ]]

        return session_frame

    # TODO(Jozef): There is some discrepancy between listed clubs and clubs used in votings,
    # should be merged into one solution somehow
    def transform_clubs(self):
        """
        Transform Clubs and Club memberships
        """
        unwind = {'$unwind': '$members'}
        projection = {
            '$project': {
                '_id': 1,
                'period_num': 1,
                'external_id': 1,
                'url': 1,
                'name': 1,
                'email': 1,
                'member_external_id': '$members.external_id',
                'membership': '$members.membership'
            }
        }
        docs = list(self._get_documents({}, unwind=unwind, projection=projection))
        print("Clubs: {}".format(len(docs)))

        club_frame = pandas.DataFrame(docs)
        if club_frame.empty:
            return club_frame

        club_frame.replace(["predsedníčka", "predseda"], 'chairman', inplace=True)
        club_frame.replace(
            ["podpredsedníčka", "podpredeseda", "podpredseda"], 'vice-chairman', inplace=True)
        club_frame.replace(["člen", "členka"], 'member', inplace=True)

        club_frame = club_frame[[
            'external_id', 'period_num', 'name', 'email', 'url',
            'member_external_id', 'membership'
        ]]
        return club_frame

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
                        'member': '$club_values',
                        'club': '$club_name'
                    },
                    'start': {
                        '$min': '$date'
                    },
                    'end': {
                        '$max': '$date'
                    }
                }
            }, {
                '$sort': {
                    '_id.member': 1,
                    'start': 1
                }
            },
            {
                '$project': {
                    'period_num': '$_id.period_num',
                    'member': '$_id.member',
                    'club': '$_id.club',
                    'start': '$start',
                    'end': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$end',
                                    max_voting
                                ]
                            },
                            'then': None,
                            'else': '$end'
                        }
                    }
                }
            }
        ]

        docs = list(self._get_documents({}, *aggregation))
        club_member_frame = pandas.DataFrame(docs)
        if club_member_frame.empty:
            return club_member_frame
        club_member_frame = club_member_frame[['period_num', 'member', 'club', 'start', 'end']]

        return club_member_frame

    def transform_votings(self):
        """
        Transform votings and votes
        """

        fields_dict = {}
        unwind = {'$unwind': '$votes'}
        projection = {
            '$project': {
                '_id': 1,
                'external_id': 1,
                'topic': 1,
                'datetime': 1,
                'session_num': 1,
                'voting_num': 1,
                'result': 1,
                'period_num': 1,
                'press_num': 1,
                'url': 1,
                'vote': '$votes.vote',
                'member_external_id': '$votes.external_id'
            }
        }
        docs = list(self._get_documents(fields_dict, unwind=unwind, projection=projection))
        print("Votes: {}".format(len(docs)))
        voting_frame = pandas.DataFrame(docs)
        if voting_frame.empty:
            return voting_frame
        voting_frame.result.replace(["Návrh prešiel"], 'passed', inplace=True)
        voting_frame.result.replace(["Návrh neprešiel"],
                                    'did_not_pass',
                                    inplace=True)
        voting_frame.result.replace(["Parlament nebol uznášaniaschopný"],
                                    'parliament_unable',
                                    inplace=True)

        voting_frame = voting_frame[[
            'external_id', 'topic', 'datetime', 'session_num', 'voting_num',
            'period_num', 'press_num', 'result', 'url', 'vote',
            'member_external_id'
        ]]
        return voting_frame



    def execute(self, context):
        """Operator Executor"""
        data_frame = None
        if self.data_type == 'member':
            data_frame = self.transform_members()
        elif self.data_type == 'member_change':
            data_frame = self.transform_member_changes()
        elif self.data_type == 'press':
            data_frame = self.transform_presses()
        elif self.data_type == 'session':
            data_frame = self.transform_sessions()
        # elif self.data_type == 'club':
        #     data_frame = self.transform_clubs()
        elif self.data_type == 'daily_club':
            data_frame = self.transform_club_members()
        elif self.data_type == 'voting':
            data_frame = self.transform_votings()

        if not data_frame.empty:
            data_frame.to_csv('{}/{}.csv'.format(self.file_dest, self.data_type), index=False)


class NRSRTransformPlugin(AirflowPlugin):

    name = 'nrsr_transform_plugin'
    operators = [NRSRTransformOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
