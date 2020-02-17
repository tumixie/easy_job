``easy_job`` can make your analysis job Easy.
======================================================================

.. code:: python

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
    from easy_job.shell.db import mysql_util
    from_file = 'path/to/csv/file'
    # format like
    # col1,col2
    # a,b
    args = ['upload',
            'mysql+pymysql://root:root@1234@localhost:3306/test',
            from_file,
            'test',
            '--mode=create',
            ]
    mysql_util.go(*args)