# coding: utf-8
"""
util.py

Usage:
  util.py extract <uri> (-t <table> | -q <query>) <to_file> [--separate=<sep>] [--log_dir=<ld>] [--log_date=<lt>]
  util.py script <uri> <from_file> [<kwargs>] [--log_dir=<ld>] [--log_date=<lt>]
  util.py upload <uri> <from_file> [<to_table>] [--separate=<sep>] [--mode=<m>] [--log_dir=<ld>] [--log_date=<lt>]
  util.py update <uri> <from_file> <to_table> <update_columns> <update_condition_cols> [--separate=<sep>] [--log_dir=<ld>] [--log_date=<lt>]
  util.py -h | --help
  util.py --version

Options:
  -h --help              Show this screen.
  --version              Show version.
  uri                    format: mysql+pymysql://[username]:[passwd]@[host]:[port]/[db]
  kwargs                 format: key1=value1,key2=value2
  --separate=<sep>       apply no file, [default: ,]
  --mode=<m>             upload mode, [default: append]
  update_columns         format: column1,column2
  update_condition_cols  format: column1,column2
"""
import os
import re
import sys
import time
import functools
import traceback
import logging
import shutil
import pathlib
from datetime import datetime

import pymysql
import pandas as pd
from docopt import docopt

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1000


def time_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        nfun = func(*args, **kwargs)
        logger.info('spend tot time: {0}s'.format(round(time.time() - start, 1)))
        return nfun

    return wrapper


@time_decorator
def extract(conn, database, table_name, to_file, query=None, sep=','):
    cursor = conn.cursor()
    sql, column_str = _get_select_query_sql(conn, database, table_name=table_name, query=query, sep=sep)
    cursor.execute(sql)
    logger.info(sql)
    results = cursor.fetchmany(CHUNK_SIZE)
    logger.info('fetch result write to file: {0}'.format(to_file))
    with open(to_file, 'w', encoding='utf-8') as of:
        of.write(column_str)
        of.write('\n')
        while results:
            for one in results:
                one = '{0}'.format(sep).join([str(x) for x in one])
                of.write(one)
                of.write('\n')
            results = cursor.fetchmany(CHUNK_SIZE)


@time_decorator
def upload(conn, table_name, from_file, sep=',', mode='append'):
    if mode == 'create':
        _create_table(conn, from_file, table_name, sep)
    generate_insert_sql(from_file, table_name, sep)
    execute_insert_many('{0}.sql'.format(from_file), conn)
    os.remove('{0}.sql'.format(from_file))


def update(conn, table_name, from_file, update_columns, update_condition_cols, sep=','):
    """

    :param conn:
    :param table_name:
    :param from_file:
    :param update_columns:
    :param update_condition_cols:
    :param sep
    :return:
    """
    generate_update_sql(from_file, table_name, update_columns, update_condition_cols, sep=sep)
    execute_insert_many('{0}.sql'.format(from_file), conn)


@time_decorator
def execute_sql_file(conn, from_file, **kwargs):
    cursor = conn.cursor()
    with open(from_file) as f:
        sql_str = f.read()
        sql_list = sql_str.split(';')
        for sql in sql_list:
            if re.sub('[\n\r ]', '', sql) == '':
                continue
            sql = sql.format(**kwargs)
            logger.info(sql)
            start_time = time.time()
            cursor.execute(sql)
            records = cursor.fetchmany(1)
            if records is not None and len(records) > 0:
                logger.info("------------result--------------")
                while records:
                    for record in records:
                        logger.info(record)
                    records = cursor.fetchmany(CHUNK_SIZE)
                logger.info("------------end--------------")
            end_time = time.time()
            logger.info('success! spend time: {0}'.format(end_time - start_time))
    cursor.close()


def _create_table(conn: pymysql.Connection, from_file: str, table_name: str, sep: str = ','):
    """
    create if upload table not exist
    :param conn:
    :param from_file:
    :param table_name:
    :param sep:
    :return:
    """
    cursor = conn.cursor()
    create_table_sql = _generate_create_sql(from_file, table_name, sep)
    cursor.execute('drop table if exists {0}'.format(table_name))
    cursor.execute(create_table_sql)
    cursor.close()


def _get_select_query_sql(conn, database, table_name=None, query=None, sep=','):
    """

    :return:
    """
    if table_name is not None:
        columns = get_table_columns(conn, database, table_name)
        columns_str = ','.join(columns)
        sql = 'select {1} from {0}'.format(table_name, columns_str)
    else:
        sql = query
    m = re.search('select(?P<cols>[\w ,]+)from', sql)
    if m is not None:
        columns_str = '{0}'.format(sep).join(m.group('cols').replace(' ', '').split(','))
    else:
        raise IllegalQuerySentence()
    return sql, columns_str


def execute_insert_many(from_sql_file, conn):
    cursor = conn.cursor()
    chunk_size = CHUNK_SIZE
    count = 0
    with open(from_sql_file, encoding='utf-8') as f:
        for ii, sql in enumerate(f):
            logger.info(sql)
            sql = sql.replace('||', '\n')
            cursor.execute(sql)
            count += 1
            if ii % chunk_size == 0:
                logger.info('chunk {0}'.format(ii // chunk_size))
                conn.commit()
        conn.commit()
    cursor.close()
    logger.info('upload {0} records.'.format(count))


def generate_insert_sql(from_file, table_name, sep=','):
    ck_df = pd.read_csv(from_file, sep=sep, chunksize=1000, dtype='str')
    template = "insert into {0} ({1}) values ({2});\n"
    with open('{0}.sql'.format(from_file), 'w', encoding='utf-8') as of:
        for df in ck_df:
            columns = ','.join([c.split('|')[0] for c in df.columns])
            s = df.apply(lambda x: ','.join(
                ["NULL" if c is None or pd.isnull(c) else "'{0}'".format(str(c)) for c in x]), axis=1)
            for line in s:
                sql = template.format(table_name, columns, line)
                of.write(sql)


def generate_update_sql(from_file, table_name, update_columns: str, update_condition_cols: str, sep=','):
    ck_df = pd.read_csv(from_file, sep=sep, chunksize=CHUNK_SIZE)
    'SET field1=new-value1, field2=new-value2'
    template = "update {0} set {1} where {2};\n"
    update_cols = update_columns.split(',')
    update_condition_cols = update_condition_cols.split(',')
    with open('{0}.sql'.format(from_file), 'w') as of:
        for df in ck_df:
            for ii, row in df.iterrows():
                update = ','.join([" {0}='{1}'".format(x, row[x]) for x in update_cols])
                condition = ' and '.join([" {0}='{1}'".format(x, row[x]) for x in update_condition_cols])
                sql = template.format(table_name, update, condition)
                of.write(sql)


def _generate_create_sql(from_file, table_name, sep=','):
    df = pd.read_csv(from_file, sep=sep)
    if '|' in df.columns[0]:  # 文件自带类型
        columns_type = [x.split('|') for x in df.columns]
    else:
        columns_type = [(k, _parse_field_type(v)) for k, v in df.dtypes.items()]
    type_str = ',\n'.join(['{0} {1} '.format(c[0], c[1]) for c in columns_type])
    sql = 'create table {0} ({1}) ENGINE=InnoDB DEFAULT CHARSET=utf8'.format(table_name, type_str)
    # sql = 'create table {0} ({1}) ENGINE=MyISAM DEFAULT CHARSET=utf8'.format(table_name, type_str)
    logger.info(sql)
    return sql


def _parse_field_type(field) -> str:
    """
    pandas 数据类型转 mysql 数据类型
    :param field:
    :return:
    """
    if str(field) == 'object':
        _type = 'char(100)'
    elif str(field) == 'bool':
        _type = 'boolean'
    else:
        _type = 'double'
    return _type


def _get_connection(uri: str) -> pymysql.Connection:
    """
    获取mysql连接
    :param uri:
    :return:
    """
    args = _parse_url(uri)
    conn = pymysql.connect(
        host=args['ipv4host'],
        database=args['database'],
        user=args['username'],
        password=args['password'],
        port=int(args['port']),
        charset='utf8',
        read_timeout=10,
        write_timeout=10
    )
    return conn


def _parse_url(uri: str):
    pattern = re.compile(r'''
            (?P<name>[\w\+]+)://
            (?:
                (?P<username>[^:/]*)
                (?::(?P<password>.*))?
            @)?
            (?:
                (?:
                    \[(?P<ipv6host>[^/]+)\] |
                    (?P<ipv4host>[^/:]+)
                )?
                (?::(?P<port>[^/]*))?
            )?
            (?:/(?P<database>.*))?
            ''', re.X)
    m = re.match(pattern, uri)
    if m is not None:
        return m.groupdict()


def get_table_columns(conn, database, table_name):
    """
    获取表的列名
    :param conn:
    :param table_name:
    :return:
    """
    cursor = conn.cursor()
    splits = table_name.split('.')
    if len(splits) == 1:
        dbname, table_name = database, splits[0]
    else:
        dbname, table_name = splits
    sql = "select column_name from information_schema.COLUMNS where table_name='{0}' and table_schema='{1}'".format(
        table_name, dbname)
    cursor.execute(sql)
    columns = []
    for one in cursor.fetchall():
        columns.append(one[0])
    return list(set(columns))


def logger_(log_file_name=None, name=__name__, stdout_on=True):
    # logger_ = logging.getLogger(name)
    # fmt = '[%(asctime)s.%(msecs)d][%(name)s][%(levelname)s]%(msg)s'
    # date_fmt = '%Y-%m-%d %H:%M:%S'
    # logging.basicConfig(filename=log_file_name, datefmt=date_fmt, format=fmt)
    # logger_.setLevel(logging.DEBUG)
    log_file_name = log_file_name if log_file_name is not None else (__name__ + '.log')

    logger_ = logging.getLogger(name)
    fmt = '%(msg)s'
    date_fmt = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(fmt=fmt, datefmt=date_fmt)

    if stdout_on:
        stout_handler = logging.StreamHandler(sys.stdout)
        stout_handler.setFormatter(formatter)
        logger_.addHandler(stout_handler)

    file_handler = logging.FileHandler(log_file_name, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger_.addHandler(file_handler)

    logger_.setLevel(logging.DEBUG)

    return logger_


def _get_log_file_name(args) -> str:
    """

    :param args:
    :return:
    """
    now_str = datetime.now().strftime('%Y%m%d%H%M%S')
    is_extract = args['extract']
    is_upload = args['upload']
    is_update = args['update']
    log_date_str = args['--log_date'] if args['--log_date'] is not None else datetime.now().strftime('%Y%m%d')
    if args['--log_dir'] is not None:
        log_dir = args['--log_dir']
    else:
        log_dir = pathlib.Path(os.getcwd(), 'log', log_date_str).as_posix()
    os.makedirs(log_dir, exist_ok=True)
    if is_extract:
        log_tail = args['<table>']
    elif is_upload or is_update:
        log_tail = args['<to_table>'] or os.path.basename(args['<from_file>'])
    else:
        log_tail = os.path.basename(args['<from_file>'])
    log_file = os.path.join(log_dir, '{0}_{1}_{2}.log'.format(now_str, log_date_str, log_tail))
    return log_file


class IllegalQuerySentence(Exception):
    pass


def go(*args):
    """

    :param kwargs:
    argv:
    log_dir:
    log_date:
    :return:
    """
    global logger
    args = docopt(__doc__, argv=args if len(args) > 0 else None)
    print(args)
    log_file = _get_log_file_name(args)
    logger = logger_(log_file, name='util')
    is_extract = args['extract']
    is_upload = args['upload']
    is_script = args['script']
    is_update = args['update']
    uri = args['<uri>']
    conn = _get_connection(uri)
    try:
        uri_parts = _parse_url(uri)
        database = uri_parts['database']
        if is_extract:
            to_file = args['<to_file>']
            table_name = args['<table>']
            query = args['<query>']
            if not args['-t'] and not args['-q']:
                raise ValueError('option [-t] or [-q] must not both False')
            sep = args['--separate']
            extract(conn, database, table_name, to_file, query=query, sep=sep)
        if is_upload:
            table_name = args['<to_table>']
            from_file = args['<from_file>']
            if table_name is None:
                table_name = os.path.basename(from_file).split('.')[0]
            sep = args['--separate']
            mode = args['--mode']
            upload(conn, table_name, from_file, sep=sep, mode=mode)
        if is_update:
            table_name = args['<to_table>']
            from_file = args['<from_file>']
            update_columns = args['<update_columns>']
            update_condition_cols = args['<update_condition_cols>']
            sep = args['--separate']
            if table_name is None:
                table_name = os.path.basename(from_file).split('.')[0]
            update(conn, table_name, from_file, update_columns=update_columns,
                   update_condition_cols=update_condition_cols, sep=sep)
        if is_script:
            from_sql_file = args['<from_file>']
            kwargs = args['<kwargs>']
            if kwargs is not None:
                kwargs = dict([k.split('=') for k in kwargs.split(',')])
            else:
                kwargs = {}
            execute_sql_file(conn, from_sql_file, **kwargs)
    except Exception as e:
        logger.error(traceback.format_exc())
        shutil.move(log_file, '{0}{1}ERROR_{2}'.format(os.path.dirname(log_file),
                                                       os.path.sep, os.path.basename(log_file)))
    finally:
        conn.close()


if __name__ == '__main__':
    go()
