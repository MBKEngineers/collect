# -*- coding: utf-8 -*-
import os
import json

from mock import patch
import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest

from collect.cnrfc import *


@pytest.fixture()
def df(): 
    return pd.read_csv('tests/2019040412_N_SanJoaquin_csv_export.csv', 
                        header=0, 
                        skiprows=[1,], 
                        nrows=60, 
                        parse_dates=True, 
                        index_col=0,
                        float_precision='high',
                        dtype={'GMT': str}) * 1000.0


def test_cnrfc_credentials():
    """
    load sensitive info from .env file and test CNRFC credentials exist
    """
    load_dotenv()
    assert ('CNRFC_USER' in os.environ) & ('CNRFC_PASSWORD' in os.environ)


def test_convert_date_columns(df):
    """Ensure datetime data converted to string format"""
    df.index = df.index.strftime('%Y-%m-%d')
    assert df.index.tolist()[0] == '2019-03-30'


def test_validate_duration():
    """
    function to properly format/case hourly or daily durations
    """
    duration = 'Hourly'
    assert validate_duration(duration) == 'hourly'
        

def test_validate_duration_invalid():
    """
    test that invalid duration raises a ValueError
    """
    bad_input = 'monthly'
    with pytest.raises(ValueError):
        validate_duration(bad_input)


def test_get_deterministic_forecast():
    """
    Test that deterministic forecast start from Graphical_RVF page matches
    CSV start of forecast
    """
    cnrfc_id = 'FOLC1'
    first_ordinate = get_forecast_meta_deterministic(cnrfc_id, first_ordinate=True)[-1]
    df = get_deterministic_forecast(cnrfc_id, truncate_historical=False)['data']
    assert df['forecast'].dropna().index.tolist()[0] == first_ordinate


def test_get_deterministic_forecast_watershed(df):
    """
    test watershed deterministic forecast download for North San Joaquin on a 
    particular date
    """
    frame = get_deterministic_forecast_watershed('N_SanJoaquin', '2019040412')['data']
    assert_frame_equal(df, frame)

# @patch('collect.class_instance.custom_method')
# def test_custom_class_method(display, data):
#     """Assert that show calls the mocked custom_method function
#     """
#     class_instance = ClassName(arg1,
#                     arg2,
#                     arg3)
#     class_instance.some_other_method()
#     custom_method.assert_called_once()
