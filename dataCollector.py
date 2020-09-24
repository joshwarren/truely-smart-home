#!/usr/bin/env python

import sys
import time

import config
import schedule
from openWeather import OpenWeather
from supply import supplier

if len(sys.argv) > 1:
    if '--include-test' in sys.argv[1]:
        print('Starting Tests')
        OpenWeather.getFreshCut()
        supplier().getFreshCut()
        config.checkForUpdatedConfig()

        print('Tests complete')

# Weather data
schedule.every().day.at('02:30').do(OpenWeather.getFreshCut)

# Micro generation

# Smart Meter

# Electricity supplier
schedule.every().day.at('03:00').do(supplier().getFreshCut)

# Config History
schedule.every().minute.do(config.checkForUpdatedConfig)

if __name__ == '__main__':
    while True:  # infinite loop
        schedule.run_pending()
        time.sleep(15)
