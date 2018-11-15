"""
Data Loader
"""

from datetime import datetime, timedelta
import logging
import os

import psycopg2

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin


class NRSRAggregateOperator(BaseOperator):
    """
    Load Data
    """

    def __init__(self, period, daily, postgres_url, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.period = period
        self.daily = daily

        self.postgres_url = postgres_url
        self.cursor = None

    def aggregate_club_bills(self):
        query = """
        SELECT PB.delivered AS "date", PC.id AS club_id, COUNT(DISTINCT PB.id) AS bill_count FROM parliament_bill PB
            INNER JOIN parliament_billproposer PBP ON PB.id = PBP.bill_id
            INNER JOIN parliament_member PM ON PBP.member_id = PM.id
            INNER JOIN parliament_clubmember PCM ON PCM.member_id = PM.id
            INNER JOIN parliament_club PC ON PCM.club_id = PC.id
        WHERE PCM.start <= PB.delivered AND (PCM.end > PB.delivered OR PCM.end IS NULL)
        GROUP BY PB.delivered, PC.id
        ORDER BY PB.delivered, PC.id
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def aggregate_club_amendments(self):
        query = """
        SELECT PA.date AS "date", APC.id AS club_id,
            COUNT(DISTINCT PA.id) FILTER (WHERE BPC.coalition IS TRUE) AS amendment_coalition,
            COUNT(DISTINCT PA.id) FILTER (WHERE BPC.coalition IS FALSE) AS amendment_opposition
            FROM parliament_amendment PA
            INNER JOIN parliament_amendmentsubmitter PAS ON PAS.amendment_id = PA.id
            INNER JOIN parliament_member APM ON PAS.member_id = APM.id
            INNER JOIN parliament_clubmember APCM ON APCM.member_id = APM.id
            INNER JOIN parliament_club APC ON APCM.club_id = APC.id

            INNER JOIN parliament_press PP ON PA.press_id = PP.id
            INNER JOIN parliament_bill PB ON PB.press_id = PP.id
            INNER JOIN parliament_billproposer PBP ON PBP.bill_id = PB.id
            INNER JOIN parliament_member BPM ON PBP.member_id = BPM.id
            INNER JOIN parliament_clubmember BPCM ON BPCM.member_id = BPM.id
            INNER JOIN parliament_club BPC ON BPCM.club_id = BPC.id

        WHERE
            APCM.start <= PA.date AND (APCM.end > PA.date OR APCM.end IS NULL)
            AND
                BPCM.start <= PB.delivered AND (BPCM.end > PB.delivered OR BPCM.end IS NULL)

        GROUP BY PA.date, APC.id, BPC.coalition
        ORDER BY PA.date, APC.id
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def aggregate_club_debate_appearances(self):
        # TODO(Jozef): learn how to get rid of floor(epoch/count)
        query = """
        SELECT DATE(PDA."start") AS "date", DPC.id AS club_id,
                CASE
                    WHEN (BPC.coalition IS TRUE) THEN (FLOOR(SUM(EXTRACT(epoch FROM (PDA."end"::timestamp - PDA."start"::timestamp))) / COUNT(PDA.id)))
                    ELSE 0
                END AS debate_seconds_coalition,
		        CASE
                    WHEN (BPC.coalition IS FALSE) THEN (FLOOR(SUM(EXTRACT(epoch FROM (PDA."end"::timestamp - PDA."start"::timestamp))) / COUNT(PDA.id)))
                    ELSE 0
                END AS debate_seconds_opposition,
                COUNT(DISTINCT PDA.id) FILTER (WHERE BPC.coalition IS TRUE) AS debate_count_coalition,
                COUNT(DISTINCT PDA.id) FILTER (WHERE BPC.coalition IS FALSE) AS debate_count_opposition
            FROM parliament_debateappearance PDA
            INNER JOIN parliament_debateappearance_press_num PDAPN ON PDAPN.debateappearance_id = PDA.id
            -- bill joins
            INNER JOIN parliament_bill PB ON PB.press_id = PDAPN.press_id
            INNER JOIN parliament_billproposer PBP ON PBP.bill_id = PB.id
            INNER JOIN parliament_member BPM ON PBP.member_id = BPM.id
            INNER JOIN parliament_clubmember BPCM ON BPCM.member_id = BPM.id
            INNER JOIN parliament_club BPC ON BPCM.club_id = BPC.id

            -- debate joins
            INNER JOIN parliament_member DPM ON PDA.debater_id = DPM.id
            INNER JOIN parliament_clubmember DPCM ON DPCM.member_id = DPM.id
            INNER JOIN parliament_club DPC ON DPCM.club_id = DPC.id

        WHERE
            DPCM.start <= PDA."start" AND (DPCM.end > PDA."start" OR DPCM.end IS NULL)
            AND
                BPCM.start <= PB.delivered AND (BPCM.end > PB.delivered OR BPCM.end IS NULL)

        GROUP BY "date", DPC.id, BPC.coalition
        ORDER BY "date", DPC.id
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def aggregate_club_interpellations(self):
        query = """
        SELECT PI.date AS "date", PC.id AS club_id,
                COUNT(DISTINCT PI.id) AS interpellation_count FROM parliament_interpellation PI
            INNER JOIN parliament_member PM ON PI.asked_by_id = PM.id
            INNER JOIN parliament_clubmember PCM ON PCM.member_id = PM.id
            INNER JOIN parliament_club PC ON PCM.club_id = PC.id
        WHERE
            PCM.start <= PI.date AND (PCM.end > PI.date OR PCM.end IS NULL)
        GROUP BY PI.date, PC.id
        ORDER BY "date", club_id
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def aggregate_club_votings(self):
        query = """
        SELECT DATE(PV.timestamp) AS "date", VPC.id AS club_id,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS TRUE AND PVV.vote = 'Z') AS voting_coalition_for,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS TRUE AND PVV.vote = 'P') AS voting_coalition_against,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS TRUE AND PVV.vote = '?') AS voting_coalition_abstain,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS TRUE AND PVV.vote = 'N') AS voting_coalition_dnv,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS TRUE AND PVV.vote = '0') AS voting_coalition_absent,

            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS FALSE AND PVV.vote = 'Z') AS voting_opposition_for,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS FALSE AND PVV.vote = 'P') AS voting_opposition_against,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS FALSE AND PVV.vote = '?') AS voting_opposition_abstain,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS FALSE AND PVV.vote = 'N') AS voting_opposition_dnv,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS FALSE AND PVV.vote = '0') AS voting_opposition_absent

            FROM parliament_voting PV
            INNER JOIN parliament_votingvote PVV ON PVV.voting_id = PV.id
            -- voter joins
            INNER JOIN parliament_member VPM ON VPM.id = PVV.voter_id
            INNER JOIN parliament_clubmember VPCM ON VPCM.member_id = VPM.id
            INNER JOIN parliament_club VPC ON VPCM.club_id = VPC.id

            -- bill join
            INNER JOIN (
                SELECT B.id, C.coalition, B.press_id
                FROM parliament_bill B
                    INNER JOIN parliament_billproposer BP ON BP.bill_id = B.id
                    INNER JOIN parliament_member M ON BP.member_id = M.id
                    INNER JOIN parliament_clubmember BCM ON BCM.member_id = M.id
                    INNER JOIN parliament_club C ON C.id = BCM.club_id
                WHERE BCM.start <= B.delivered AND (BCM.end >= B.delivered OR BCM.end IS NULL)
                GROUP BY B.id, C.coalition
            ) BILL ON BILL.press_id = PV.press_id

        WHERE
            VPCM.start <= DATE(PV.timestamp) AND (VPCM.end >= DATE(PV.timestamp) OR VPCM.end IS NULL)
        GROUP BY "date", VPC.id
        ORDER BY "date", club_id
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def insert_aggregates(self, values_list):
        query = """
        INSERT INTO parliament_stats_clubstats ({columns})
        VALUES ({values})
        ON CONFLICT DO NOTHING;
        """

        for values in values_list:
            insert_columns = []
            insert_values = []
            for key, value in values.items():
                insert_columns.append(key)
                insert_values.append(value)

            self.cursor.execute(
                query.format(
                    columns=', '.join(insert_columns),
                    values=', '.join(map(str, insert_values))
                ))

    def execute(self, context):
        """Operator Executor"""
        pg_conn = psycopg2.connect(self.postgres_url)
        self.cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        aggregate_dict = {
            'bill_count': 0,
            'amendment_coalition': 0,
            'amendment_opposition': 0,
            'amendment_government': 0,
            'amendment_committee': 0,
            'debate_count_coalition': 0,
            'debate_count_opposition': 0,
            'debate_count_government': 0,
            'debate_count_committee': 0,
            'debate_member_count': 0,
            'debate_seconds_coalition': 0,
            'debate_seconds_opposition': 0,
            'debate_seconds_government': 0,
            'debate_seconds_committee': 0,
            'interpellation_count': 0,
            'voting_coalition_for': 0,
            'voting_coalition_against': 0,
            'voting_coalition_abstain': 0,
            'voting_coalition_dnv': 0,
            'voting_coalition_absent': 0,
            'voting_opposition_for': 0,
            'voting_opposition_against': 0,
            'voting_opposition_abstain': 0,
            'voting_opposition_dnv': 0,
            'voting_opposition_absent': 0,
            'voting_government_for': 0,
            'voting_government_against': 0,
            'voting_government_abstain': 0,
            'voting_government_dnv': 0,
            'voting_government_absent': 0,
            'voting_committee_for': 0,
            'voting_committee_against': 0,
            'voting_committee_abstain': 0,
            'voting_committee_dnv': 0,
            'voting_committee_absent': 0,
        }
        try:
            aggregates = {}
            # bills
            for obj in self.aggregate_club_bills():
                if obj['date'] not in aggregates:
                    aggregates[obj['date']] = {}
                if obj['club_id'] not in aggregates[obj['date']]:
                    aggregates[obj['date']][obj['club_id']] = aggregate_dict.copy()
                
                aggregates[obj['date']][obj['club_id']] = {**aggregates[obj['date']][obj['club_id']], **obj}

            # amendments
            for obj in self.aggregate_club_amendments():
                if obj['date'] not in aggregates:
                    aggregates[obj['date']] = {}
                if obj['club_id'] not in aggregates[obj['date']]:
                    aggregates[obj['date']][obj['club_id']] = aggregate_dict.copy()
                
                aggregates[obj['date']][obj['club_id']] = {**aggregates[obj['date']][obj['club_id']], **obj}

            # debate appearances
            for obj in self.aggregate_club_debate_appearances():
                if obj['date'] not in aggregates:
                    aggregates[obj['date']] = {}
                if obj['club_id'] not in aggregates[obj['date']]:
                    aggregates[obj['date']][obj['club_id']] = aggregate_dict.copy()
                
                aggregates[obj['date']][obj['club_id']] = {**aggregates[obj['date']][obj['club_id']], **obj}

            # interpellations
            for obj in self.aggregate_club_interpellations():
                if obj['date'] not in aggregates:
                    aggregates[obj['date']] = {}
                if obj['club_id'] not in aggregates[obj['date']]:
                    aggregates[obj['date']][obj['club_id']] = aggregate_dict.copy()
                
                aggregates[obj['date']][obj['club_id']] = {**aggregates[obj['date']][obj['club_id']], **obj}

            # votings
            for obj in self.aggregate_club_votings():
                if obj['date'] not in aggregates:
                    aggregates[obj['date']] = {}
                if obj['club_id'] not in aggregates[obj['date']]:
                    aggregates[obj['date']][obj['club_id']] = aggregate_dict.copy()
                
                aggregates[obj['date']][obj['club_id']] = {**aggregates[obj['date']][obj['club_id']], **obj}

            values_list = []
            for day, clubs in aggregates.items():
                for club, values in clubs.items():
                    # values_list.append({**values, **{'"date"': "'{}'".format(day.strftime('%Y-%m-%d')), 'club_id': club}})
                    values['date'] = "'{}'".format(values['date'].strftime('%Y-%m-%d'))
                    values_list.append(values)
            self.insert_aggregates(values_list)
            pg_conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            raise error
        finally:
            if pg_conn is not None:
                pg_conn.close()


class NRSRAggregatePlugin(AirflowPlugin):

    name = 'nrsr_aggregate_plugin'
    operators = [NRSRAggregateOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
