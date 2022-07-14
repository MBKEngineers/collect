Install latest version of java on your environment here:
https://www.java.com/en/download/help/mac_install.html

Installing Tabula:

pip install tabula-py


HOW TO USE:

From collect.cvo.cvo_dout import file_getter_dout

Ex.

If start date is 1st of January 2015
End date is 20th of April 2022

*Use date time format in (YYYY/MM/DD)

file_getter_dout(datetime.datetime(2015,1,10), datetime.datetime(2022,4,20))

