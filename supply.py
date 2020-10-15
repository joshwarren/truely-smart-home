import base64
import json

import pandas as pd
import requests

from config import electricalSupplier, dbConfig
from db import db
import octopus_tariff_app as octopus

with db(**dbConfig) as DB:
    DB.create_schema('supply')


class supplier:
    baseURL: str
    key: str
    productRef: str
    tariffDetails_URL: str
    DB = db(**dbConfig)

    def __init__(self):
        self.__dict__.update(electricalSupplier)
        self.key = base64.b64encode(self.key.encode()).decode()

    @property
    def get_tariff(self):
        if self.supplier == 'Octopus Energy':
            return octopus.get_tariff

    @property
    def get_usage(self):
        if self.supplier == 'Octopus Energy':
            return octopus.get_usage

    @property
    def get_export(self):
        if self.supplier == 'Octopus Energy':
            return octopus.get_export

    def getFreshCut(self):
        tariff = self.get_tariff(electricalSupplier['productRef'])
        self.DB.dataframe_to_table(tariff, 'tariff', schema='supply',
                                   dedup=True)

        usage = self.get_usage()
        self.DB.dataframe_to_table(usage, 'consumption', schema='supply',
                                   dedup=True)

        export = self.get_export()
        self.DB.dataframe_to_table(export, 'exported', schema='supply',
                                   dedup=True)
