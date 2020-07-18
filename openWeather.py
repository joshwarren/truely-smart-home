import datetime
import json

import pandas as pd
import requests

from config import lat, lon, openWeather, dbConfig
from db import db

with db(**dbConfig) as DB:
    DB.create_schema('weather')


class OpenWeather:
    DB = db(**dbConfig)

    @classmethod
    def getFreshCut(cls):
        data = requests.get(
            f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={openWeather['key']}&units=metric")

        forcast = pd.DataFrame.from_dict(json.loads(data.text)['hourly'])
        forcast = forcast.join(pd.DataFrame.from_dict(
            list(forcast.weather.apply(lambda x: x[0]))))
        forcast.drop('weather', axis=1, inplace=True)
        forcast.rename(columns={'id': 'weatherId'}, inplace=True)
        forcast.dt = forcast.dt.apply(
            lambda x: datetime.datetime.fromtimestamp(x))
        for precipitation in ['rain', 'snow']:
            if precipitation in forcast.columns:
                forcast[precipitation] = forcast[precipitation].apply(
                    lambda x: x['1h'] if type(x) is dict else x)
        forcast['forcastDate'] = datetime.datetime.now()

        self.DB.dataframe_to_table(forcast, 'forecast', schema='weather')
