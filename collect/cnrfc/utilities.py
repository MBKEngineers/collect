"""
collect.cnrfc.utilities
============================================================

"""
# -*- coding: utf-8 -*-
import datetime as dt
import math
import pandas as pd
from collect.cnrfc import *


def rank_by_peak_average_flow(df, horizon, exceedences):
    """
    placeholder

    Args:
        df (pandas.DataFrame): description
        horizon (str): description
        exceedences (list): description

    Returns:
        (list)
    """

    # average x-day flows (x=horizon)
    avg = pd.DataFrame.rolling(df[::-1], window=horizon, min_periods=0).mean()[::-1][start:end]

    # dictionary of ens. member id to max x-day avg flow in horizon
    peaks = [(k, round(max(v), 2)) for (k, v) in avg.iteritems()]

    return peaks


def get_ranked_members(df, duration='H', horizon=5, exceedences=[10, 50, 90]):
    """
    assign max envelope, min envelope, and 5%, 10%, 25%, 50% exceedence members
    horizon (days) | int | 5, 30

    Args:
        df (pandas.DataFrame): description
        duration (str): description
        horizon (int): description
        exceedences (list): description

    Returns:
        (dict)
    """

    # start and end of ranking horizon
    start = min(df.index).to_pydatetime()
    end = start + dt.timedelta(days=horizon)

    # for hourly data, convert horizon from days to hours
    cfs_to_taf = 24 * 3600.0 / 43560000.0
    if duration[0].upper() == 'H':
        horizon = 24 * horizon
        cfs_to_taf = 3600.0 / 43560000.0

    # forward-looking cumulative volumes
    sums = df.cumsum().head(horizon).tail(1) * cfs_to_taf

    # list of tuples: (ensemble member id, cumulative volume)
    vols = list(sums.to_dict(orient='records')[0].items())

    # rank the cumulative flows
    ranked = sorted(vols, key=lambda value: value[1], reverse=True)
    n = len(vols)

    # determine member ids matching each of the exceedences specified
    keys = {x: ranked[int(math.ceil(n * x/100.0)) - 1] for x in exceedences}

    # add max and min entries
    keys.update({'max': ranked[0], 'min': ranked[-1]})

    return keys
