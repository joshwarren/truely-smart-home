#!/usr/bin/python3

# From: https://github.com/AciDCooL/Domoticz-SolaxCloud-Script.git
# See also: https://community.home-assistant.io/t/solax-solar-inverter-setup-guide/48008/82


import requests
import sys
import time
import pandas as pd
import numpy as np
from typing import Union

from config import microgen, dbConfig
from db import db

with db(**dbConfig) as DB:
    DB.create_schema('microgen')

    sql = """
    CREATE TABLE IF NOT EXISTS microgen.technologies (
        type VARCHAR(20)
        , make VARCHAR(20)
        , sn VARCHAR(100)
        , instance_no INT
    )
    """
    DB.session.execute(sql)
    DB.session.commit()


class Microgen_base:
    techType: str

    def __init__(self, make: str, config: dict, instanceNo: int):
        self.make = make
        self.config = config
        self.instanceNo = instanceNo

    @property
    def tableName(self):
        # Postgres works better with lower case
        return f"{self.techType}_{self.make}_{self.instanceNo}".lower()


class Solar(Microgen_base):
    techType = 'Solar'

    def getRealTimeData(self):
        response = requests.get(
            f"{self.config['API_URL']}/getRealtimeInfo.do",
            params={'tokenId': self.config['key'],
                    'sn': self.config['SN']})
        realTimeData = pd.DataFrame.from_records(
            response.json()['result'], index=[0])
        realTimeData['uploadTime'] = pd.to_datetime(
            realTimeData['uploadTime']).dt.tz_localize('Europe/London')

        return realTimeData


class Microgen:
    def __init__(self):
        self.technologies = pd.DataFrame.from_records(microgen)
        self.technologies['sn'] = [tech['cloud']['SN'] for tech in microgen]
        self.technologies['instance_no'] = self.technologies.apply(
            lambda row: self._get_instance_no(row['type'], row['make'],
                                              row['sn']), axis=1)
        self.technologies['object'] = self.technologies.apply(
            lambda row: eval(row['type'])(row['make'], row['cloud'],
                                          row['instance_no']), axis=1)

        for idx, tech in self.technologies.iterrows():
            assert tech['type'] in globals().keys(), \
                f"Tech type of {tech['type']} has not yet been implimented"

            with db(**dbConfig) as DB:
                DB.dataframe_to_table(
                    tech.to_frame().T[['type', 'make', 'sn',
                                       'instance_no']], 'technologies', 'microgen',
                    dedup=True)

    def _get_instance_no(self, techType: str, make: str, SN: str
                         ) -> Union[int, np.ndarray]:
        """
        Instance no is unique id device of a given type and make so that a user might have multple Solax solar PV systems. Unique systems are identified by SN.
        """

        with db(**dbConfig) as DB:
            current_tech = pd.read_sql_table(
                'technologies', DB.connection, schema='microgen')

        instance_no = current_tech[(current_tech['type'] == techType)
                                   * (current_tech['make'] == make)
                                   * (current_tech['sn'] == SN)
                                   ]
        assert len(
            instance_no) < 2, "Multiple instances of the same microgen technologies have been found in microgen.technologies table."

        if len(instance_no) == 1:
            # tech has been seen before - use previous instance no
            return instance_no['instance_no'].values[0]

        # tech has not been seen before
        # Find same make and type that have been seen before
        instance_no = current_tech[
            (current_tech['type'] == techType)
            * (current_tech['make'] == make)
        ]

        def insert_new_instance_no(instance_no: int):
            sql = f"""
                INSERT INTO microgen.technologies(type, make, sn, instance_no)
                VALUES ({techType}, {make}, {SN}, {instance_no})
            """
        if len(instance_no) > 0:
            # Iterate on previously seen tech
            instance_no = instance_no['instance_no'].max() + 1
        else:
            instance_no = 0
        insert_new_instance_no(instance_no)
        return instance_no

    def getRealTimeData(self):
        for idx, tech in self.technologies.iterrows():
            data = tech.object.getRealTimeData()

            with db(**dbConfig) as DB:
                DB.dataframe_to_table(data, tech.object.tableName,
                                      "microgen", dedup=True)
