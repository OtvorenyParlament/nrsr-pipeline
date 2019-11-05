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

    def aggregate_club_amendments_govandcom(self):
        """
        Aggregate Amendments to government and committees bills
        """
        query = """
        SELECT PA.date AS "date", APC.id AS club_id,
            COUNT(DISTINCT PA.id) FILTER (WHERE PB.proposer_type = 1) AS amendment_government,
            COUNT(DISTINCT PA.id) FILTER (WHERE PB.proposer_type = 2) AS amendment_committee
            FROM parliament_amendment PA
            INNER JOIN parliament_amendmentsubmitter PAS ON PAS.amendment_id = PA.id
            INNER JOIN parliament_member APM ON PAS.member_id = APM.id
            INNER JOIN parliament_clubmember APCM ON APCM.member_id = APM.id
            INNER JOIN parliament_club APC ON APCM.club_id = APC.id
            INNER JOIN parliament_press PP ON PA.press_id = PP.id
            INNER JOIN parliament_bill PB ON PB.press_id = PP.id

        WHERE
            APCM.start <= PA.date AND (APCM.end > PA.date OR APCM.end IS NULL)
            AND PB.proposer_type IN (1, 2)

        GROUP BY PA.date, APC.id
        ORDER BY PA.date, APC.id
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()


    def aggregate_club_debate_appearances(self):
        # TODO(Jozef): learn how to get rid of floor(epoch/count)
        query = """
        SELECT DATE(PDA."start") AS "date", DPC.id AS club_id,
                -- broken second calculations, skip for now
                0 AS debate_seconds_coalition,
                0 AS debate_seconds_opposition,
                --CASE
                --    WHEN (BPC.coalition IS TRUE) THEN (FLOOR(SUM(EXTRACT(epoch FROM (PDA."end"::timestamp - PDA."start"::timestamp))) / COUNT(PDA.id)))
                --    ELSE 0
                --END AS debate_seconds_coalition,
		         --CASE
                --    WHEN (BPC.coalition IS FALSE) THEN (FLOOR(SUM(EXTRACT(epoch FROM (PDA."end"::timestamp - PDA."start"::timestamp))) / COUNT(PDA.id)))
                --    ELSE 0
                --END AS debate_seconds_opposition,
                COUNT(DISTINCT PDA.id) FILTER (WHERE BPC.coalition IS TRUE) AS debate_count_coalition,
                COUNT(DISTINCT PDA.id) FILTER (WHERE BPC.coalition IS FALSE) AS debate_count_opposition,
                COUNT(DISTINCT DPM.id) FILTER (WHERE BPC.coalition IS TRUE) AS debater_count_coalition,
		        COUNT(DISTINCT DPM.id) FILTER (WHERE BPC.coalition IS FALSE) AS debater_count_opposition
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
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS TRUE AND PVV.vote = 0) AS voting_coalition_for,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS TRUE AND PVV.vote = 1) AS voting_coalition_against,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS TRUE AND PVV.vote = 2) AS voting_coalition_abstain,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS TRUE AND PVV.vote = 3) AS voting_coalition_dnv,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS TRUE AND PVV.vote = 4) AS voting_coalition_absent,
            --COUNT(PVV.id) FILTER (WHERE BILL.coalition IS TRUE AND PVV.vote = -1) AS voting_coalition_invalid,

            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS FALSE AND PVV.vote = 0) AS voting_opposition_for,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS FALSE AND PVV.vote = 1) AS voting_opposition_against,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS FALSE AND PVV.vote = 2) AS voting_opposition_abstain,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS FALSE AND PVV.vote = 3) AS voting_opposition_dnv,
            COUNT(PVV.id) FILTER (WHERE BILL.coalition IS FALSE AND PVV.vote = 4) AS voting_opposition_absent--,
            --COUNT(PVV.id) FILTER (WHERE BILL.coalition IS FALSE AND PVV.vote = -1) AS voting_opposition_invalid

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

    def aggregate_club_votings_govandcom(self):
        """
        Aggregate Club votings for gov and committee bills
        """
        query = """
        SELECT DATE(PV.timestamp) AS "date", VPC.id AS club_id,
            COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 1 AND PVV.vote = 0) AS voting_government_for,
            COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 1 AND PVV.vote = 1) AS voting_government_against,
            COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 1 AND PVV.vote = 2) AS voting_government_abstain,
            COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 1 AND PVV.vote = 3) AS voting_government_dnv,
            COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 1 AND PVV.vote = 4) AS voting_government_absent,
            --COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 1 AND PVV.vote = -1) AS voting_government_invalid,

            COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 2 AND PVV.vote = 0) AS voting_committee_for,
            COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 2 AND PVV.vote = 1) AS voting_committee_against,
            COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 2 AND PVV.vote = 2) AS voting_committee_abstain,
            COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 2 AND PVV.vote = 3) AS voting_committee_dnv,
            COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 2 AND PVV.vote = 4) AS voting_committee_absent--,
            --COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 2 AND PVV.vote = -1) AS voting_committee_invalid

            FROM parliament_voting PV
            INNER JOIN parliament_votingvote PVV ON PVV.voting_id = PV.id
            -- voter joins
            INNER JOIN parliament_member VPM ON VPM.id = PVV.voter_id
            INNER JOIN parliament_clubmember VPCM ON VPCM.member_id = VPM.id
            INNER JOIN parliament_club VPC ON VPCM.club_id = VPC.id

            -- bill join
            INNER JOIN parliament_bill BILL ON BILL.press_id = PV.press_id

        WHERE
            VPCM.start <= DATE(PV.timestamp) AND (VPCM.end >= DATE(PV.timestamp) OR VPCM.end IS NULL)
            AND BILL.proposer_type IN (1, 2)
        GROUP BY "date", VPC.id
        ORDER BY "date", club_id
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def insert_club_aggregates(self, values_list):
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

    def insert_member_aggregates(self, values_list):
        query = """
        INSERT INTO parliament_stats_memberstats ({columns})
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

    def insert_global_aggregates(self, values_list):
        query = """
        INSERT INTO parliament_stats_globalstats ({columns})
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

    def aggregate_member_bills(self):
        query = """
        SELECT PB.delivered AS "date", PM.id AS member_id, COUNT(DISTINCT PB.id) AS bill_count FROM parliament_bill PB
            INNER JOIN parliament_billproposer PBP ON PB.id = PBP.bill_id
            INNER JOIN parliament_member PM ON PBP.member_id = PM.id
        GROUP BY PB.delivered, PM.id
        ORDER BY PB.delivered, PM.id
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def aggregate_member_amendments(self):
        query = """
        SELECT PA.date AS "date", APM.id AS member_id,
            COUNT(DISTINCT PA.id) AS amendment_count
            FROM parliament_amendment PA
            INNER JOIN parliament_amendmentsubmitter PAS ON PAS.amendment_id = PA.id
            INNER JOIN parliament_member APM ON PAS.member_id = APM.id

        GROUP BY PA.date, APM.id
        ORDER BY PA.date, APM.id
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def aggregate_member_interpellations(self):
        query = """
        SELECT PI.date AS "date", PM.id AS member_id,
                COUNT(DISTINCT PI.id) AS interpellation_count FROM parliament_interpellation PI
            INNER JOIN parliament_member PM ON PI.asked_by_id = PM.id
        GROUP BY PI.date, PM.id
        ORDER BY "date", member_id
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def aggregate_member_debate_appearances(self):
        # TODO(Jozef): learn how to get rid of floor(epoch/count)
        query = """
        SELECT DATE(PDA."start") AS "date", PDA.debater_id AS member_id,
                0 AS debate_seconds,
                COUNT(DISTINCT PDA.id) AS debate_count
            FROM parliament_debateappearance PDA
        WHERE PDA.debater_id IS NOT NULL
        GROUP BY "date", member_id
        ORDER BY "date", member_id
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()


    def aggregate_club_stats(self):
        """
        Aggregate Club Stats
        """
        aggregate_club_dict = {
            'bill_count': 0,
            'amendment_coalition': 0,
            'amendment_opposition': 0,
            'amendment_government': 0,
            'amendment_committee': 0,
            'debater_count_coalition': 0,
            'debater_count_opposition': 0,
            'debater_count_government': 0,
            'debater_count_committee': 0,
            'debate_count_coalition': 0,
            'debate_count_opposition': 0,
            'debate_count_government': 0,
            'debate_count_committee': 0,
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
            #'voting_coalition_invalid': 0,
            'voting_opposition_for': 0,
            'voting_opposition_against': 0,
            'voting_opposition_abstain': 0,
            'voting_opposition_dnv': 0,
            'voting_opposition_absent': 0,
            #'voting_opposition_invalid': 0,
            'voting_government_for': 0,
            'voting_government_against': 0,
            'voting_government_abstain': 0,
            'voting_government_dnv': 0,
            'voting_government_absent': 0,
            #'voting_government_invalid': 0,
            'voting_committee_for': 0,
            'voting_committee_against': 0,
            'voting_committee_abstain': 0,
            'voting_committee_dnv': 0,
            'voting_committee_absent': 0,
            #'voting_committee_invalid': 0,
        }

        ##### club aggregates
        club_aggregates = {}
        # bills
        for obj in self.aggregate_club_bills():
            if obj['date'] not in club_aggregates:
                club_aggregates[obj['date']] = {}
            if obj['club_id'] not in club_aggregates[obj['date']]:
                club_aggregates[obj['date']][obj['club_id']] = aggregate_club_dict.copy()
            
            club_aggregates[obj['date']][obj['club_id']] = {**club_aggregates[obj['date']][obj['club_id']], **obj}

        # amendments
        for obj in self.aggregate_club_amendments():
            if obj['date'] not in club_aggregates:
                club_aggregates[obj['date']] = {}
            if obj['club_id'] not in club_aggregates[obj['date']]:
                club_aggregates[obj['date']][obj['club_id']] = aggregate_club_dict.copy()
            
            club_aggregates[obj['date']][obj['club_id']] = {**club_aggregates[obj['date']][obj['club_id']], **obj}
        
        for obj in self.aggregate_club_amendments_govandcom():
            if obj['date'] not in club_aggregates:
                club_aggregates[obj['date']] = {}
            if obj['club_id'] not in club_aggregates[obj['date']]:
                club_aggregates[obj['date']][obj['club_id']] = aggregate_club_dict.copy()
            
            club_aggregates[obj['date']][obj['club_id']] = {**club_aggregates[obj['date']][obj['club_id']], **obj}

        # debate appearances
        for obj in self.aggregate_club_debate_appearances():
            if obj['date'] not in club_aggregates:
                club_aggregates[obj['date']] = {}
            if obj['club_id'] not in club_aggregates[obj['date']]:
                club_aggregates[obj['date']][obj['club_id']] = aggregate_club_dict.copy()
            
            club_aggregates[obj['date']][obj['club_id']] = {**club_aggregates[obj['date']][obj['club_id']], **obj}

        # interpellations
        for obj in self.aggregate_club_interpellations():
            if obj['date'] not in club_aggregates:
                club_aggregates[obj['date']] = {}
            if obj['club_id'] not in club_aggregates[obj['date']]:
                club_aggregates[obj['date']][obj['club_id']] = aggregate_club_dict.copy()
            
            club_aggregates[obj['date']][obj['club_id']] = {**club_aggregates[obj['date']][obj['club_id']], **obj}

        # votings
        for obj in self.aggregate_club_votings():
            if obj['date'] not in club_aggregates:
                club_aggregates[obj['date']] = {}
            if obj['club_id'] not in club_aggregates[obj['date']]:
                club_aggregates[obj['date']][obj['club_id']] = aggregate_club_dict.copy()
            
            club_aggregates[obj['date']][obj['club_id']] = {**club_aggregates[obj['date']][obj['club_id']], **obj}

        for obj in self.aggregate_club_votings_govandcom():
            if obj['date'] not in club_aggregates:
                club_aggregates[obj['date']] = {}
            if obj['club_id'] not in club_aggregates[obj['date']]:
                club_aggregates[obj['date']][obj['club_id']] = aggregate_club_dict.copy()
            
            club_aggregates[obj['date']][obj['club_id']] = {**club_aggregates[obj['date']][obj['club_id']], **obj}

        club_values_list = []
        for day, clubs in club_aggregates.items():
            for club, values in clubs.items():
                # values_list.append({**values, **{'"date"': "'{}'".format(day.strftime('%Y-%m-%d')), 'club_id': club}})
                values['date'] = "'{}'".format(values['date'].strftime('%Y-%m-%d'))
                club_values_list.append(values)
        self.insert_club_aggregates(club_values_list)

    def aggregate_member_stats(self):
        """
        Aggregate member stats
        """
        aggregate_member_dict = {
            'bill_count': 0,
            'amendment_count': 0,
            'interpellation_count': 0,
            'debate_count': 0,
            'debate_seconds': 0,
        }

        member_aggregates = {}
        # bills
        for obj in self.aggregate_member_bills():
            if obj['date'] not in member_aggregates:
                member_aggregates[obj['date']] = {}
            if obj['member_id'] not in member_aggregates[obj['date']]:
                member_aggregates[obj['date']][obj['member_id']] = aggregate_member_dict.copy()
            
            member_aggregates[obj['date']][obj['member_id']] = {**member_aggregates[obj['date']][obj['member_id']], **obj}

        # amendments
        for obj in self.aggregate_member_amendments():
            if obj['date'] not in member_aggregates:
                member_aggregates[obj['date']] = {}
            if obj['member_id'] not in member_aggregates[obj['date']]:
                member_aggregates[obj['date']][obj['member_id']] = aggregate_member_dict.copy()
            
            member_aggregates[obj['date']][obj['member_id']] = {**member_aggregates[obj['date']][obj['member_id']], **obj}

        # debate appearances
        for obj in self.aggregate_member_debate_appearances():
            if obj['date'] not in member_aggregates:
                member_aggregates[obj['date']] = {}
            if obj['member_id'] not in member_aggregates[obj['date']]:
                member_aggregates[obj['date']][obj['member_id']] = aggregate_member_dict.copy()
            
            member_aggregates[obj['date']][obj['member_id']] = {**member_aggregates[obj['date']][obj['member_id']], **obj}

        # interpellations
        for obj in self.aggregate_member_interpellations():
            if obj['date'] not in member_aggregates:
                member_aggregates[obj['date']] = {}
            if obj['member_id'] not in member_aggregates[obj['date']]:
                member_aggregates[obj['date']][obj['member_id']] = aggregate_member_dict.copy()
            
            member_aggregates[obj['date']][obj['member_id']] = {**member_aggregates[obj['date']][obj['member_id']], **obj}
        
        member_values_list = []
        for day, members in member_aggregates.items():
            for member, values in members.items():
                values['date'] = "'{}'".format(values['date'].strftime('%Y-%m-%d'))
                member_values_list.append(values)
        self.insert_member_aggregates(member_values_list)

    def aggregate_global_bills_members(self):
        """Aggregate global bills"""
        query = """
        SELECT PB.delivered AS "date",
            PC.period_id AS period_id,
            COUNT(DISTINCT PB.id) FILTER (WHERE PC.coalition IS TRUE) AS bill_count_by_coalition,
            COUNT(DISTINCT PB.id) FILTER (WHERE PC.coalition IS FALSE) AS bill_count_by_opposition
            FROM parliament_bill PB
            INNER JOIN parliament_billproposer PBP ON PB.id = PBP.bill_id
            INNER JOIN parliament_member PM ON PBP.member_id = PM.id
            INNER JOIN parliament_clubmember PCM ON PCM.member_id = PM.id
            INNER JOIN parliament_club PC ON PCM.club_id = PC.id
        WHERE PCM.start <= PB.delivered AND (PCM.end > PB.delivered OR PCM.end IS NULL)
        AND PB.proposer_type = 0
        GROUP BY PC.period_id, PB.delivered
        ORDER BY PC.period_id, PB.delivered
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def aggregate_global_bills_govandcom(self):
        """Aggregate global bills"""
        query = """
        SELECT PB.delivered AS "date",
            PP.period_id AS period_id,
            COUNT(DISTINCT PB.id) FILTER (WHERE PB.proposer_type = 1) AS bill_count_by_government,
            COUNT(DISTINCT PB.id) FILTER (WHERE PB.proposer_type = 2) AS bill_count_by_committee
            FROM parliament_bill PB
            INNER JOIN parliament_press PP ON PP.id = PB.press_id
        WHERE PB.proposer_type IN (1, 2)
        GROUP BY PP.period_id, PB.delivered
        ORDER BY PP.period_id, PB.delivered
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def aggregate_global_amendments(self):
        """Aggregate global amendments"""
        query = """
        SELECT PA.date AS "date",
            APC.period_id AS period_id,
            COUNT(DISTINCT PA.id) FILTER (WHERE APC.coalition IS TRUE) AS amendment_count_by_coalition,
            COUNT(DISTINCT PA.id) FILTER (WHERE APC.coalition IS FALSE) AS amendment_count_by_opposition
            FROM parliament_amendment PA
            INNER JOIN parliament_amendmentsubmitter PAS ON PAS.amendment_id = PA.id
            INNER JOIN parliament_member APM ON PAS.member_id = APM.id
            INNER JOIN parliament_clubmember APCM ON APCM.member_id = APM.id
            INNER JOIN parliament_club APC ON APCM.club_id = APC.id

        WHERE
            APCM.start <= PA.date AND (APCM.end > PA.date OR APCM.end IS NULL)

        GROUP BY APC.period_id, PA.date
        ORDER BY APC.period_id, PA.date
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def aggregate_global_interpellations(self):
        query = """
        SELECT PI.date AS "date",
                PC.period_id AS period_id,
                COUNT(DISTINCT PI.id) FILTER (WHERE PC.coalition IS TRUE) AS interpellation_count_by_coalition,
                COUNT(DISTINCT PI.id) FILTER (WHERE PC.coalition IS FALSE) AS interpellation_count_by_opposition
        FROM parliament_interpellation PI
            INNER JOIN parliament_member PM ON PI.asked_by_id = PM.id
            INNER JOIN parliament_clubmember PCM ON PCM.member_id = PM.id
            INNER JOIN parliament_club PC ON PCM.club_id = PC.id
        WHERE
            PCM.start <= PI.date AND (PCM.end > PI.date OR PCM.end IS NULL)
        GROUP BY PC.period_id, "date"
        ORDER BY PC.period_id, "date"
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def aggregate_global_stats(self):
        """
        Aggregate Global Stats
        """
        aggregate_dict = {
            'bill_count_by_coalition': 0,
            'bill_count_by_opposition': 0,
            'bill_count_by_government': 0,
            'bill_count_by_committee': 0,
            'amendment_count_by_coalition': 0,
            'amendment_count_by_opposition': 0,
            'interpellation_count_by_coalition': 0,
            'interpellation_count_by_opposition': 0,
        }
        aggregates = {}
        # bills
        for obj in self.aggregate_global_bills_members():
            if obj['date'] not in aggregates:
                aggregates[obj['date']] = {}
            if obj['period_id'] not in aggregates[obj['date']]:
                aggregates[obj['date']][obj['period_id']] = aggregate_dict.copy()
            
            aggregates[obj['date']][obj['period_id']] = {**aggregates[obj['date']][obj['period_id']], **obj}

        for obj in self.aggregate_global_bills_govandcom():
            if obj['date'] not in aggregates:
                aggregates[obj['date']] = {}
            if obj['period_id'] not in aggregates[obj['date']]:
                aggregates[obj['date']][obj['period_id']] = aggregate_dict.copy()
            
            aggregates[obj['date']][obj['period_id']] = {**aggregates[obj['date']][obj['period_id']], **obj}

        # amendments
        for obj in self.aggregate_global_amendments():
            if obj['date'] not in aggregates:
                aggregates[obj['date']] = {}
            if obj['period_id'] not in aggregates[obj['date']]:
                aggregates[obj['date']][obj['period_id']] = aggregate_dict.copy()
            
            aggregates[obj['date']][obj['period_id']] = {**aggregates[obj['date']][obj['period_id']], **obj}

        # interpellations
        for obj in self.aggregate_global_interpellations():
            if obj['date'] not in aggregates:
                aggregates[obj['date']] = {}
            if obj['period_id'] not in aggregates[obj['date']]:
                aggregates[obj['date']][obj['period_id']] = aggregate_dict.copy()
            
            aggregates[obj['date']][obj['period_id']] = {**aggregates[obj['date']][obj['period_id']], **obj}
        
        values_list = []
        for day, periods in aggregates.items():
            for period, values in periods.items():
                values['date'] = "'{}'".format(values['date'].strftime('%Y-%m-%d'))
                values_list.append(values)
        self.insert_global_aggregates(values_list)

    def aggregate_session_govbills(self):

        query = """
        SELECT SESS.id AS session_id, SESS.session_num AS session_num, SESS.name AS session_name,
            COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 1 AND PVV.vote = 0) AS gov_bills_for_votes_by_coalition,
            COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 1 AND PVV.vote = 1) AS gov_bills_against_votes_by_coalition,
            COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 1 AND PVV.vote = 2) AS gov_bills_abstain_votes_by_coalition,
            COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 1 AND PVV.vote = 3) AS gov_bills_dnv_votes_by_coalition,
            COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 1 AND PVV.vote = 4) AS gov_bills_absent_votes_by_coalition,
            COUNT(PVV.id) FILTER (WHERE BILL.proposer_type = 1 AND PVV.vote = -1) AS gov_bills_invalid_votes_by_coalition

            FROM parliament_voting PV
            INNER JOIN parliament_votingvote PVV ON PVV.voting_id = PV.id
            -- voter joins
            INNER JOIN parliament_member VPM ON VPM.id = PVV.voter_id
            INNER JOIN parliament_clubmember VPCM ON VPCM.member_id = VPM.id
            INNER JOIN parliament_club VPC ON VPCM.club_id = VPC.id

            -- bill join
            INNER JOIN parliament_bill BILL ON BILL.press_id = PV.press_id

	    -- session join
	    INNER JOIN parliament_session SESS ON SESS.id = PV.session_id

        WHERE
            VPCM.start <= DATE(PV.timestamp) AND (VPCM.end >= DATE(PV.timestamp) OR VPCM.end IS NULL)
            AND BILL.proposer_type IN (1, 2)
            AND VPC.coalition IS TRUE
        GROUP BY SESS.id
        ORDER BY SESS.id
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def aggregate_session_stats(self):
        aggregate_dict = {
            'gov_bills_count': 0,
            'gov_bills_for_votes_by_coalition': 0,
            'gov_bills_against_votes_by_coalition': 0,
            'gov_bills_abstain_votes_by_coalition': 0,
            'gov_bills_dnv_votes_by_coalition': 0,
            'gov_bills_absent_votes_by_coalition': 0,
            'gov_bills_invalid_votes_by_coalition': 0,
            'gov_bills_for_votes_by_opposition': 0,
            'gov_bills_against_votes_by_opposition': 0,
            'gov_bills_abstain_votes_by_opposition': 0,
            'gov_bills_dnv_votes_by_opposition': 0,
            'gov_bills_absent_votes_by_opposition': 0,
            'gov_bills_invalid_votes_by_coalition': 0,
        }

        aggregates = {}
        # bills
        for obj in self.aggregate_session_govbills():
            if obj['session_id'] not in aggregates:
                aggregates[obj['session_id']] = aggregate_dict.copy()
            aggregates[obj['session_id']] = {**aggregates[obj['session_id']], **obj}
        

    def execute(self, context):
        """Operator Executor"""
        pg_conn = psycopg2.connect(self.postgres_url)
        self.cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            ##### club aggregates
            self.aggregate_club_stats()

            ##### member aggregates
            self.aggregate_member_stats()

            ##### global aggregates
            self.aggregate_global_stats()

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
