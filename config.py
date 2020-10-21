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


def updateConfigs(config):
    config = pd.Series(config)
    config['configChangedAt'] = datetime.datetime.now()

    config = config.to_frame().T.astype(str)

    with db(**dbConfig) as DB:
        dtype = {'configchangedat': sqlalchemy.types.DateTime(),
                 'lat': sqlalchemy.types.Float(),
                 'lon': sqlalchemy.types.Float()}

        DB.dataframe_to_table(config, 'config_history', schema='config',
                              dtype=dtype)


def checkForUpdatedConfig():
    with open('./config.json') as file:
        configs = json.load(file)

    with db(**dbConfig) as DB:
        has_changed = DB.has_changed(configs, 'config_history', 'config',
                                     'configchangedat')
    if has_changed:
        updateConfigs(configs)
