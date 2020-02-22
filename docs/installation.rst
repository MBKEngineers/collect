.. role:: bash(code)
   :language: bash

Setup instructions
================================================================


Create a virtual environment
----------------------------------------

Create a virtual environment, specifying Python version >=3.6::

   $ mkvirtualenv --python=python3 collect

.. note:: This depends on the location/name of your Python 3 executable. On windows, check the location of your Python installation with ```where```.


Download source code
----------------------------------------
Download the project source code from https://github.com/MBKEngineers/collect.git::

   $ git clone https://github.com/MBKEngineers/collect.git


Install package requirements
----------------------------------------
Install package requirements to the active virtual environment with ```pip``` or ```pip3```::

   $ cd collect
   $ python -m pip install -r requirements.txt
  
.. note:: If ```pip3``` comes packaged with your Python installation, this is another option::
   
   $ pip3 install -r requirements.txt


Install `collect`
----------------------------------------
Install `collect` as a Python package available for local use.  Use the ```setup.py``` file to install `collect` to the active virtual environment::

   $ python collect/setup.py develop


Configure package variables
----------------------------------------
Create a `.env` file containing the path to installed ChromeDriver for use with Selenium:

```CHROMEDRIVER=/usr/local/bin/chromedriver```
