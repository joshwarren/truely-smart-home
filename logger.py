"""Custom logger to write to DB and file.

Based on the following solution:
https://stackoverflow.com/a/43843623/6709902
"""

import logging
import sys
import traceback

from config import dbConfig, logConfig, pushNotifications
from db import db
import pushover


log_schema = 'log'
log_table = 'log'

with db(**dbConfig) as DB:
    DB.create_schema(log_schema)

    sql = """
        CREATE TABLE IF NOT EXISTS log.log (
            id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
            "timestamp" timestamptz NULL DEFAULT now(),
            logged_by varchar(100) NULL,
            log_level int2 NULL,
            log_level_name varchar(10) NULL,
            log_message text NULL
        );
    """
    DB.session.execute(sql)
    DB.session.commit()


class LogDBHandler(logging.Handler):
    '''
    Customized logging handler that puts logs to the database.
    pymssql required
    '''
    client = pushover.Client(pushNotifications['client'],
                             api_token=pushNotifications['token'])

    def __init__(self, DB, db_tbl_log: str):
        super().__init__()
        self.DB = DB
        self.db_tbl_log = db_tbl_log

    def emit(self, record):
        # Clear the log message so it can be put to db via sql (escape quotes)
        self.log_msg = record.msg
        self.log_msg = self.log_msg.strip()
        self.log_msg = self.log_msg.replace('\'', '\'\'')
        # Make the SQL insert
        sql = f"""INSERT INTO log.{self.db_tbl_log} (
                logged_by
                , log_level
                , log_level_name
                , log_message)
            VALUES (
                '{record.name}'
                , {record.levelno}
                , '{record.levelname}'
                , '{self.log_msg}')"""
        try:
            self.DB.session.execute(sql)
            self.DB.session.commit()
        # If error - print it out on screen. Since DB is not working - there's
        # no point making a log about it to the database :)
        except:  # pymssql.Error as e:
            print(sql)
            print('CRITICAL DB ERROR! Logging to database not possible!')

        if record.levelno >= 40 and logConfig['push_errors']:
            self.client.send_message(self.log_msg,
                                     title="ERROR detected in Truely Smart Home App")


# Set file logger
logging.basicConfig(filename=logConfig['log_file_path'])

# Main settings for the database logging use
if logConfig['log_to_db']:
    # Make the connection to database for the logger
    DB = db(**dbConfig)
    logdb = LogDBHandler(DB, log_table)

    # Set db handler for root logger
    logging.getLogger('').addHandler(logdb)

# Register logger
logger = logging.getLogger('Truely-Smart-Home')
logger.setLevel(logConfig['log_error_level'])

if logConfig['log_exceptions']:
    def log_exceptions(exctype, value, tb):
        # logger.exception(
        # f"UNCAUGHT EXCEPTION: Type: {exctype}, Value: {value}, Traceback: {tb}")
        # traceback.print_exception(exctype, value, tb)
        exception = ''.join(traceback.format_exception(exctype, value, tb))

        logger.exception(exception)

    sys.excepthook = log_exceptions
