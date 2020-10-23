#!/usr/bin/env python

import sys
import time

import schedule

import config
from action import action
from microGeneration import Microgen
from octopus_tariff_app import immersion_on_during_cheapest_period, push_tariff
from openWeather import OpenWeather
from supply import supplier
from logger import logger

if len(sys.argv) > 1:
    if '--include-test' in sys.argv[1]:
        logger.info('Starting Tests')

        logger.info('Testing OpenWeather module...')
        OpenWeather.getFreshCut()

        logger.info('Testing Supplier module...')
        supplier().getFreshCut()

        logger.info('Testing Config module...')
        config.checkForUpdatedConfig()

        logger.info('Testing Octopus Tariff App module...')
        push_tariff()
        immersion_on_during_cheapest_period()

        logger.info('Testing Action module...')
        action().execute_todo()

        logger.info('Testing Microgen module...')
        Microgen().getRealTimeData()

        logger.info('Tests complete')

# Weather data
schedule.every().day.at('02:30').do(OpenWeather.getFreshCut)

# Micro generation
microgen = Microgen()
schedule.every(5).minutes.do(microgen.getRealTimeData)

# Smart Meter/Electricity supplier
schedule.every().day.at('03:00').do(supplier().getFreshCut)

# Octopus tariff
schedule.every().day.at('01:00').do(push_tariff)
schedule.every().day.at('01:00').do(immersion_on_during_cheapest_period)

# Config History
schedule.every(5).minutes.do(config.checkForUpdatedConfig)

# Actions to be done
schedule.every(5).minutes.do(action().execute_todo)


if __name__ == '__main__':
    while True:  # infinite loop
        schedule.run_pending()
        time.sleep(15)
