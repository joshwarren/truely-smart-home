import base64
import json

import pandas as pd
import requests

from config import electricalSupplier
from db import db

with db(**dbConfig) as DB:
    DB.create_schema('supply')


class supplier:
    baseURL: str
    key: str
    productRef: str
    tariffDetails_URL: str

    def __init__(self):
        self.__dict__.update(electricalSupplier)
        self.key = base64.b64encode(self.key.encode()).decode()

        if self.supplier == 'Octopus Energy':
            self._setup_Octopus()

    def _setup_Octopus(self):
        self.baseURL = 'https://api.octopus.energy/v1'

        self.tariffDetails_URL = f"{self.baseURL}/products/{self.productRef}/electricity-tariffs/E-1R-{self.productRef}-C/standard-unit-rates/"

    def getFreshCut(self):
        tariff = requests.get(self.tariffDetails_URL)
        tariff = pd.DataFrame.from_records(json.loads(tariff.text)['results'])
        with db(**dbConfig, engine='SQLAlchemy') as DB:
            tariff.to_sql('tariff', DB.connection, schema='supply',
                          index=False, if_exists='append')
