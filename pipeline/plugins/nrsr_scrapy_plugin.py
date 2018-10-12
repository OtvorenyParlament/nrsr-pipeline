"""
Scrapy Plugin
"""

import logging
import os
from subprocess import CalledProcessError, check_call

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin


class NRSRScrapyOperator(BaseOperator):
    """
    Scrapy Operator
    """

    def __init__(self, spider, scrapy_home, period, daily, *args, **kwargs):
        """
        if you need only particular period, put period=int
        if you want full crawl (a lot of time and data), make full=True
        """
        super().__init__(*args, **kwargs)

        self.scrapy_home = scrapy_home
        self.scrapy_bin = '{}/env/bin/scrapy'.format(self.scrapy_home)

        self.spider = spider
        self.period = period
        self.daily = daily

    def execute(self, context):
        """Operator Executor"""
        shell_cmd = ['{} crawl {}'.format(self.scrapy_bin, self.spider)]
        print("Daily: {}".format(self.daily))
        print("Period: {}".format(self.period))
        if self.daily is not None:
            shell_cmd.append('-a daily={}'.format('true' if self.daily is True else 'false'))
        if self.period is not None:
            shell_cmd.append('-a period={}'.format(self.period))
        wd = os.getcwd()
        os.chdir(self.scrapy_home)
        cmd_string = ' '.join(shell_cmd)
        print("Executing command: {}".format(cmd_string))
        check_call(cmd_string, shell=True)
        os.chdir(wd)


class NRSRScrapyPlugin(AirflowPlugin):

    name = 'nrsr_scrapy_plugin'
    operators = [NRSRScrapyOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
