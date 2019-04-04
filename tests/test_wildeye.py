# -*- coding: utf-8 -*-
import os
import json

from mock import patch
import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest

from dotenv import load_dotenv


def test_wildeye_credentials():
    """
    load sensitive info from .env file and test Wildeye credentials exist
    """
    load_dotenv()
    assert ('WILDEYE_USER' in os.environ) & ('WILDEYE_PASSWORD' in os.environ)

