# collect
Web scraping utilties for DWR, USACE, USGS, CNRFC, CVO and SacALERT data repositories.

## Setup instructions
### Create a virtual environment, specifying Python version 3.8

#### With pure Python (3+)
Create a virtual environment with Python 3's built-in `venv` library.  
```
$ python -m venv ~/.virtualenvs/myenv
```
Activate with 
```$ myenv\Scripts\activate.bat``` (Windows)
or ```$ source myenv/bin/activate``` (MacOS).

#### Other virtualenv managers
- _pyenv_
- _virtualenvwrapper_

### Download the source code for this package.
```$ git clone https://github.com/MBKEngineers/collect.git```

### Install `collect` as a Python package available for local use.
Use the "develop" flag to make sure changes in your repo are propagated to any use of your virtualenv.
```
$ cd collect
$ python setup.py develop
```
or with _pip_ and the "editable" flag (-e):
```
$ cd collect
$ pip install -e .
```

### Configure package variables
Add username and password credentials to a `.env` file to enable downloading data from password-protected sources.

### Updating Documentation
The `collect` module uses Sphinx to generate documentation from doc-strings in the project.  To update and access documentation files, make sure that Sphinx is installed:
```
$ python -m pip install -e ".[docs]"
```

### Namespace
Note, there is one other Python package on PyPi named  `collect`.  However, it is not maintained and is dated 2011, so not expecting MBK codebase to use that tool.

### Adding new modules
`collect` now includes a command line interface for starting a new module called `collect-start`. Initialize a new module from a template with
```
$ collect-start modulename
```
