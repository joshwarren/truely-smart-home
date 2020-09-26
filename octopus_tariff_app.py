import json
from typing import Tuple

import matplotlib as mpl
import matplotlib.pyplot as plt
import mplcyberpunk
import numpy as np
import pandas as pd
import pushover
import requests

from config import pushNotifications


def get_tariff(product_code: str) -> pd.DataFrame:
    baseURL = 'https://api.octopus.energy/v1'
    agileProduct = requests.get(
        f"{baseURL}/products/{productCode}/electricity-tariffs/E-1R-{productCode}-L/standard-unit-rates/")
    tariff = pd.DataFrame.from_records(
        json.loads(agileProduct.text)['results'])

    # default times are UTC
    tariff['valid_from'] = pd.to_datetime(
        tariff['valid_from']).dt.tz_convert('Europe/London')
    tariff['valid_to'] = pd.to_datetime(
        tariff['valid_to']).dt.tz_convert('Europe/London')

    # tariff['diff'] = -tariff.value_inc_vat.diff(periods=1)

    return tariff


def plot_tariff(tariff: pd.DataFrame, timeFrom_series: str, timeTo_series: str, value_series: str, saveTo: str = None) -> pd.DataFrame:
    plt.style.use("cyberpunk")

    diff = -tariff[value_series].diff(periods=1)

    fig, ax = plt.subplots()
    ax.step(tariff[timeFrom_series], tariff[value_series])

    # Set x axis labels
    freq = '3H'
    t = pd.date_range(tariff[timeFrom_series].min().ceil(freq),
                      tariff[timeFrom_series].max(), freq=freq)
    ax.set_ylabel('Tariff (p/kWh)')
    ax.set_xticks(t)
    ax.set_xticklabels([time.time().strftime("%I %p").lstrip('0')
                        for time in t], rotation=90)

    def label(label_series: pd.Series, text_offset: Tuple[float]):
        # Wrapper for annotate method

        xlim = [mpl.dates.num2date(l) for l in ax.get_xlim()]
        xaxis_range = (xlim[1] - xlim[0]).total_seconds() / 60 / 60

        strftime = "%I %p" if label_series[timeTo_series].strftime(
            "%M") == "00" else "%I:%M %p"
        x_shift_hrs = text_offset[0] * xaxis_range
        ax.annotate(label_series[timeTo_series].strftime(strftime).lstrip('0'),
                    xy=(label_series[timeTo_series],
                        label_series[value_series] + diff / 2),
                    xytext=(label_series[timeTo_series] + pd.Timedelta(
                        f'{x_shift_hrs} hours'), (label_series[value_series]
                                                  + diff / 2) * text_offset[1]),
                    arrowprops={'arrowstyle': '->'})

    # table start and end times of large jumps in price
    for big_step in tariff[diff > 10].iterrows():
        big_step = big_step[1]
        label(big_step, (-0.1 if big_step[timeTo_series].strftime(
            "%M") == "00" else -0.12, 1.1))

    for big_step in tariff[diff < -10].iterrows():
        big_step = big_step[1]
        label(big_step, (0.04, 1.1))

    mplcyberpunk.make_lines_glow()
    mplcyberpunk.add_underglow()

    if saveTo is not None:
        plt.savefig(saveTo, bbox_inches='tight')
    else:
        return ax


def push_tariff():
    client = pushover.Client(pushNotifications['client'],
                             api_token=pushNotifications['token'])

    productCode = 'AGILE-18-02-21'
    tariff = get_tariff(productCode)
    plot_tariff(tariff, 'valid_from', 'valid_to',
                'value_inc_vat', saveTo='octopus_tariff.png')

    start_time = tariff.valid_from.min().strftime(
        "%I %p %a %d %b" if tariff.valid_from.min().strftime(
            "%M") == "00" else "%I:%M %p %a %d %b").lstrip("0")
    end_time = tariff.valid_from.max().strftime(
        "%I %p %a %d %b" if tariff.valid_from.max().strftime(
            "%M") == "00" else "%I:%M %p %a %d %b").lstrip("0")

    with open('octopus_tariff.png', 'rb') as attachment:
        client.send_message(f"{start_time} to {end_time}",
                            title="Octopus Tariff", attachment=attachment)


if __name__ == '__main__':
    push_tariff()