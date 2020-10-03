#!/usr/bin/env python

import sys
import time

import schedule

import config
from action import action
from octopus_tariff_app import immersion_on_during_cheapest_period, push_tariff
from openWeather import OpenWeather
from supply import supplier

if len(sys.argv) > 1:
    if '--include-test' in sys.argv[1]:
        print('Starting Tests')

        print('Testing OpenWeather module...')
        OpenWeather.getFreshCut()

        print('Testing Supplier module...')
        supplier().getFreshCut()

        print('Testing Config module...')
        config.checkForUpdatedConfig()

        print('Testing Octopus Tariff App module...')
        push_tariff()
        immersion_on_during_cheapest_period()

        print('Testing Action module...')
        action().execute_todo()

        print('Tests complete')

# Weather data
schedule.every().day.at('02:30').do(OpenWeather.getFreshCut)

# Micro generation

# Smart Meter

# Electricity supplier
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
