"""
Data Loader
"""

from datetime import datetime, timedelta
import logging
import os
from subprocess import CalledProcessError, check_call

from bson.objectid import ObjectId
import pandas
import psycopg2
from pymongo import MongoClient

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

        self.postgres_url = postgres_url

    def load_members(self):
        """
        Load MPs
        """

        pass

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
