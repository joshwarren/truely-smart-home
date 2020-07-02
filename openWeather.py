import datetime
import json

import pandas as pd
import requests

from config import lat, lon, openWeather, dbConfig
from db import db

with db(**dbConfig) as DB:
    DB.create_schema('weather')


class OpenWeather:

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
        forcast.rain = forcast.rain.apply(
            lambda x: x['1h'] if type(x) is dict else x)
        forcast['forcastDate'] = datetime.datetime.now()

        with db(**dbConfig, engine='SQLAlchemy') as DB:
            forcast.to_sql('forecast', DB.connection, schema='weather',
                           index=False, if_exists='append')

    # @classmethod
    # def on_next(cls):
    #     """Pseudonym for getFreshCut for use by RxPy"""
    #     # cls.getFreshCut()
    #     print('yep')

    # def on_error(error):
    #     pass

    # def on_completed():
    #     pass
