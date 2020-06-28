import datetime
import json

import pandas as pd
import sqlalchemy
from typing import List
import pyodbc

from db import db

configFile = './config.json'


def loadConfig():
    """Load from config.json"""
    with open(configFile) as file:
        globals().update(json.load(file))


loadConfig()

with db(**dbConfig) as DB:
    DB.create_schema('config')


def hasChanged(newConfig: dict) -> bool:
    with db(**dbConfig) as DB:
        try:
            oldConfig = pd.read_sql("""
                SELECT *
                FROM config.config_history
                ORDER BY "configChangedAt" DESC
                LIMIT 1
                """, DB.connection).astype(str)
            del oldConfig['configChangedAt']
        except pyodbc.Error:
            # table does not exist yet
            oldConfig = pd.DataFrame()

    newConfig = pd.Series(newConfig)
    newConfig = newConfig.to_frame().T.astype(str)

    return not oldConfig.equals(newConfig)


def checkNewConfigs(configs: List):
    """ Check for new config keys
    Method extends table if new keys are found
    """

    with db(**dbConfig) as DB:
        currentFields = DB.connection.execute(f"""SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'config'
                AND table_name = 'config_history'
                """)
        currentFields = [f[0] for f in currentFields.fetchall()]

        if len(currentFields) == 0:
            # table does not exist yet
            return

        newFields = [f for f in configs if f not in currentFields]
        for field in newFields:
            DB.connection.execute(f"""
                ALTER TABLE config.config_history
                ADD COLUMN {field} TEXT NULL
                """)


def updateConfigs(config):
    config = pd.Series(config)
    config['configChangedAt'] = datetime.datetime.now()

    config = config.to_frame().T.astype(str)

    checkNewConfigs(config.columns)

    with db(**dbConfig, engine='SQLAlchemy') as DB:
        config.to_sql('config_history', DB.connection, schema='config',
                      index=False, if_exists='append',
                      dtype={'configChangedAt': sqlalchemy.types.DateTime,
                             'lat': sqlalchemy.types.Float,
                             'lon': sqlalchemy.types.Float})


def checkForUpdatedConfig():
    with open('./config.json') as file:
        configs = json.load(file)

    if hasChanged(configs):
        updateConfigs(configs)
