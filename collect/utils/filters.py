import numpy as np
from scipy.signal import argrelextrema


def filter_peaks(df, column_name, threshold, order=5):
    """
    identify local maxima that are measurement or data entry faults and replace with None
    """

    # find local peaks (returns a np array; we want first element)
    locator = argrelextrema(df[column_name].values, np.greater_equal, order=order)[0]

    # create peaks series from column_name by locating index with peaks
    peaks = df.iloc[locator][column_name]

    # filter local peaks above a threshold
    df.loc[peaks > threshold, column_name] = None

    return df