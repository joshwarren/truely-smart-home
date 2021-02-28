import base64
import json
from datetime import datetime
from typing import Tuple

import matplotlib as mpl
import matplotlib.pyplot as plt
import mplcyberpunk
import numpy as np
import pandas as pd
import pushover
import pytz
import requests

from config import (dbConfig, electricalSupplier, pushNotifications)
from db import db
# from logger import create_logger

# logger = create_logger('octopus_tariff_app')


def get_tariff(productCode: str) -> pd.DataFrame:
    agileProduct = requests.get(
        f"{electricalSupplier['API_URL']}/products/{productCode}/electricity-tariffs/E-1R-{productCode}-L/standard-unit-rates/")
    tariff = pd.DataFrame.from_records(
        json.loads(agileProduct.text)['results'])

    # default times are UTC
    tariff['valid_from'] = pd.to_datetime(
        tariff['valid_from']).dt.tz_convert('Europe/London')
    tariff['valid_to'] = pd.to_datetime(
        tariff['valid_to']).dt.tz_convert('Europe/London')

    return tariff


def get_usage():
    return get_usage_base(electricalSupplier["MPAN"])


def get_export():
    return get_usage_base(electricalSupplier["MPAN_export"])


def get_usage_base(MPAN) -> pd.DataFrame:
    token = base64.b64encode(electricalSupplier['key'].encode()).decode()
    response = requests.get(
        f'{electricalSupplier["API_URL"]}/electricity-meter-points/{MPAN}/meters/{electricalSupplier["serialNo"]}/consumption/', headers={"Authorization": f'Basic {token}'})

    usage = pd.DataFrame.from_records(json.loads(response.text)['results'])

    # default times are UTC
    usage['interval_start'] = pd.to_datetime(
        usage['interval_start']).dt.tz_convert('Europe/London')
    usage['interval_end'] = pd.to_datetime(
        usage['interval_end']).dt.tz_convert('Europe/London')

    return usage


def get_cheapest_period(n: int = 1):
    tariff = get_tariff(electricalSupplier['productRef'])
    tariff = tariff[tariff['valid_to'] >
                    datetime.now(pytz.timezone('Europe/London'))]

    return tariff.sort_values('value_inc_vat', ascending=True).head(n)


def create_actions(df: pd.DataFrame, start: str = 'From', end: str = 'To', start_action: str = 'on', end_action: str = 'off') -> pd.DataFrame:
    """Transform table with From and To fields to actions

    Args:
        df (pd.DataFrame): Dataframe with schema which includes start and end time fields for actions
        start (str, optional): name of start time field. Defaults to 'From'.
        end (str, optional): name of end time field. Defaults to 'To'.
        start_action (str, optional): Action at start. None results in no action set. Defaults to 'on'.
        end_action (str, optional): As start_action. Defaults to 'off'.

    Returns:
        pd.DataFrame: [description]
    """

    # Discard unneeded fields
    df = df[[start, end]]

    action = pd.melt(df).sort_values(['value', 'variable'])

    # set actions
    action['action'] = start_action
    action['action'][action['variable'] == end] = end_action

    # Drop None values in action field
    action.dropna(subset=['action'], inplace=True)

    # prepare to return
    action.rename(columns={'value': 'action_time'}, inplace=True)
    action.drop('variable', axis=1, inplace=True)

    return action


def immersion_on_during_cheapest_period():
    # logger.info(
    #     'Running immersion_on_during_cheapest_period() from octopus_tariff_app')

    set_n_periods = electricalSupplier['auto_immersion_periods']
    cheapest_period = get_cheapest_period(set_n_periods)

    action = create_actions(
        cheapest_period, start='valid_from', end='valid_to')
    action['device_id'] = 'Immersion'

    with db(**dbConfig) as DB:
        device_type = DB.lookup_table('device_type', 'action', index='id')
        action['device_type'] = device_type.Shelly.value

        DB.dataframe_to_table(action, 'action', schema='action')


def is_edge_nan(arr: np.ndarray) -> np.ndarray:
    """
    Returns boolean array idenfiying outer nans in group. e.g.

    $ is_edge_nan([1, nan, nan, nan, 3, nan, nan])
    >>> [False, True, False, True, False, True, False]
    """

    return np.isnan(arr) \
        * (np.append(~np.isnan(arr[1:]), False)  # i+1 nan?
            + np.append(False, ~np.isnan(arr[:-1]))  # i-1 nan?
           )


def split_array(x: np.ndarray, y: np.ndarray, split: float, above: bool = True
                ) -> Tuple[np.ndarray, np.ndarray]:
    """Split array at a given y value. Used to change color of plot at e.g. y = 0

    Args:
        x (np.ndarray): x value of plot
        y (np.ndarray): y value of plot
        split (float): value when split is to occur
        above (bool, optional): If True, returns y array above split. Defaults to True.

    Returns:
        Tuple[np.ndarray, np.ndarray]: x and y to plot. NB: both x and y have been over sampled to ensure that correct gaps are plotted by matplotlib.
    """

    # Over sample
    i = np.arange(len(x) * 2, step=2)
    i_interp = np.arange(len(x) * 2)

    dtype = x.dtype  # Allows np.interp to handle datetime dtypes
    x = np.interp(i_interp, i, x.astype(float)).astype(dtype)

    dtype = y .dtype
    y = np.interp(i_interp, i, y.astype(float)).astype(dtype)

    # Apply split
    if above:
        y[y <= split] = np.nan
    else:
        y[y > split] = np.nan

    # go to boundary
    y[is_edge_nan(y)] = split

    return x, y


def plot_tariff(tariff: pd.DataFrame, timeFrom_series: str, timeTo_series: str, value_series: str, saveTo: str = None) -> pd.DataFrame:
    plt.style.use("cyberpunk")

    tariff['diff'] = -tariff[value_series].diff(periods=1)

    fig, ax = plt.subplots()

    # Create step functions
    timeFrom = tariff[timeFrom_series].repeat(2).values[:-1]
    value = tariff[value_series].repeat(2).values[1:]

    x, pos = split_array(timeFrom, value, 0, True)
    _, neg = split_array(timeFrom, value, 0, False)

    ax.plot(x, pos, color='c')
    ax.plot(x, neg, color='r')

    # Set x axis labels
    freq = '3H'
    t = pd.date_range(tariff[timeFrom_series].min().ceil(freq),
                      tariff[timeFrom_series].max(), freq=freq)
    ax.set_ylabel('Tariff (p/kWh)')
    ax.set_xticks(t)
    ax.set_xticklabels([time.time().strftime("%I %p").lstrip('0')
                        for time in t], rotation=90)

    def label(label_series: pd.Series, text_offset: Tuple[float, float]):
        # Wrapper for annotate method
        xlim = [mpl.dates.num2date(l) for l in ax.get_xlim()]
        xaxis_range = (xlim[1] - xlim[0]).total_seconds() / 60 / 60

        strftime = "%I %p" if label_series[timeTo_series].strftime(
            "%M") == "00" else "%I:%M %p"
        x_shift_hrs = text_offset[0] * xaxis_range

        ax.annotate(label_series[timeTo_series].strftime(strftime).lstrip('0'),
                    xy=(label_series[timeTo_series],
                        label_series[value_series] + label_series['diff'] / 2),
                    xytext=(label_series[timeTo_series] + pd.Timedelta(
                        f'{x_shift_hrs} hours'), (label_series[value_series]
                                                  + label_series['diff'] / 2) * text_offset[1]),
                    arrowprops={'arrowstyle': '->'})

    # table start and end times of large jumps in price
    for big_step in tariff[tariff['diff'] > 10].iterrows():
        big_step = big_step[1]
        label(big_step, (-0.1 if big_step[timeTo_series].strftime(
            "%M") == "00" else -0.12, 1.1))

    for big_step in tariff[tariff['diff'] < -10].iterrows():
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

    tariff = get_tariff(electricalSupplier['productRef'])
    plot_tariff(tariff, 'valid_from', 'valid_to',
                'value_inc_vat', saveTo='octopus_tariff.png')

    start_time = tariff.valid_from.min().strftime(
        "%I %p %a %d %b" if tariff.valid_from.min().strftime(
            "%M") == "00" else "%I:%M %p %a %d %b").lstrip("0")
    end_time = tariff.valid_from.max().strftime(
        "%I %p %a %d %b" if tariff.valid_from.max().strftime(
            "%M") == "00" else "%I:%M %p %a %d %b").lstrip("0")

    with open('octopus_tariff.png', 'rb') as attachment:
        try:
            # logger.info('Pushing tariff plot to pushover')
            client.send_message(f"{start_time} to {end_time}",
                                title="Octopus Tariff", attachment=attachment)
        except requests.exceptions.ConnectionError:
            # Error connecting to pushover api
            # logger.error(f'Error connecting to PushOver API from {__file__}.')
            pass


if __name__ == '__main__':
    push_tariff()
    # immersion_on_during_cheapest_period()
