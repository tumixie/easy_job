# coding: utf-8

import os
import unittest
import pathlib

from easy_job.tests import DATA_SET_DIR, LOG_DIR
from easy_job.shell.db import mysql_util


# from_file = '/Users/tumixie/project/ffd/ds/root/project/job/20191217_taiping_pboc_debt/0115/test_loan.txt'
# log_dir = '/Users/tumixie/project/ffd/ds/root/project/job/20191217_taiping_pboc_debt/0115'
# log_date = '20200213'
# args = ['upload', from_file, 'loan_all', '--log_dir={0}'.format(log_dir), '--log_date={0}'.format(log_date),
#         '--mode=create']
# mysql_util.go(*args)


class TestMysqlUtil(unittest.TestCase):
    def setUp(self) -> None:
        self.from_file = DATA_SET_DIR.joinpath('test_loan.txt').as_posix()
        self.update_file = DATA_SET_DIR.joinpath('test_update.txt').as_posix()
        self.from_sql_file = DATA_SET_DIR.joinpath('test_loan.sql').as_posix()
        self.to_file = DATA_SET_DIR.joinpath('out_test_loan.txt').as_posix()
        LOG_DIR.mkdir(exist_ok=True)
        self.log_date = '20200217'

    def tearDown(self) -> None:
        pass

    def test_upload(self):
        args = ['upload',
                'mysql+pymysql://root:root@1234@localhost:3306/test',
                self.from_file,
                'loan_all',
                '--log_dir={0}'.format(LOG_DIR.as_posix()),
                '--log_date={0}'.format(self.log_date),
                '--mode=create',
                '--sep=\t']
        mysql_util.go(*args)

    def test_extract(self):
        args1 = ['extract',
                 'mysql+pymysql://root:root@1234@localhost:3306/test',
                 '-t',
                 'loan_all',
                 self.to_file,
                 '--log_dir={0}'.format(LOG_DIR.as_posix()),
                 '--log_date={0}'.format(self.log_date),
                 '--sep=\t'
                 ]
        mysql_util.go(*args1)

        args2 = ['extract',
                 'mysql+pymysql://root:root@1234@localhost:3306/test',
                 '-q',
                 'select months, account from test.loan_all limit 2',
                 self.to_file,
                 '--log_dir={0}'.format(LOG_DIR.as_posix()),
                 '--log_date={0}'.format(self.log_date),
                 '--sep=\t']
        mysql_util.go(*args2)

    def test_script(self):
        args1 = ['script',
                 'mysql+pymysql://root:root@1234@localhost:3306/test',
                 self.from_sql_file,
                 'limit1=6,limit2=7',
                 '--log_dir={0}'.format(LOG_DIR.as_posix()),
                 '--log_date={0}'.format(self.log_date),
                 ]
        mysql_util.go(*args1)

    def test_update(self):
        args = ['update',
                'mysql+pymysql://root:root@1234@localhost:3306/test',
                self.update_file,
                'test2',
                'trade_id,txn_id',
                'account,open_days',
                '--log_dir={0}'.format(LOG_DIR.as_posix()),
                '--log_date={0}'.format(self.log_date),
                '--sep=\t',
                ]
        mysql_util.go(*args)


if __name__ == '__main__':
    unittest.main()
