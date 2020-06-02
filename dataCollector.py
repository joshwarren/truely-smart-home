import time

import schedule
from openWeather import OpenWeather
from supply import supplier
import config

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
        time.sleep(1)
