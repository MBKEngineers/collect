"""
collect.cvo
============================================================
access cvo data

HOW TO USE:

From collect.cvo.cvo_dout import file_getter_dout

Ex.

If start date is 1st of January 2015
End date is 20th of April 2022

*Use date time format in (YYYY/MM/DD)

file_getter_dout(datetime.datetime(2015,1,10), datetime.datetime(2022,4,20))

"""
# -*- coding: utf-8 -*-
from . import cvo_dout, cvo_shadop, cvo_kesdop, cvo_shafln
