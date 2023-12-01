.. role:: bash(code)
   :language: bash

Setup instructions
================================================================


Create a virtual environment
----------------------------------------
Create a virtual environment, specifying Python version >=3.6::

   $ python -m venv ~/.virtualenvs/collect

.. note:: This depends on the location/name of your Python 3 executable. On windows, check the location of your Python installation with ``where``. On MacOS, use ``which``.


Download source code
----------------------------------------
Download the project source code from https://github.com/MBKEngineers/collect.git::

   $ git clone https://github.com/MBKEngineers/collect.git


Install ``collect``
----------------------------------------
Install ``collect`` to the active virtual environment as a Python package available for local use.  Use the ``setup.py`` file to install ``collect`` to the active virtual environment. Using the ``develop`` flag ensures any changes made to your local repository propagate to the virtual environment. To freeze the version of ``collect`` installed to the environment, use the ``install`` flag.::

   $ cd collect
   $ python setup.py develop

Or, install package requirements to the active virtual environment with ``pip`` or ``pip3``::

   $ cd collect
   $ python -m pip install -e .


Updating Documentation
----------------------------------------
The collect module uses Sphinx to generate documentation from doc-strings in the project. This dependency is included as an extra. To update and access documentation files, make sure that Sphinx is installed::

   $ python -m pip install -e ".[docs]"


Configure package variables
----------------------------------------
Create a ``.env`` file containing module secrets and configuration details. If accessing password-protected data, include your credentials in the ``.env`` file.::

   CNRFC_USER=
   CNRFC_PASSWORD=
   WATERBOARDS_USER=
   WATERBOARDS_PASSWORD=


Namespace
----------------------------------------
Note, there is one other Python package on PyPi named  ``collect``.  However, it is not maintained and is dated 2011, so not expecting MBK codebase to use that tool.


Adding new modules
----------------------------------------
``collect`` now includes a command line interface for starting a new module called ``collect-start``. Initialize a new module from a template with::

   $ collect-start modulename
