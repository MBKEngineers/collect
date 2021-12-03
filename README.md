# collect
Web scraping utilties for DWR, USACE, USGS, CNRFC, CVO and SacALERT data repositories.

## Setup instructions
### Create a virtual environment, specifying Python version >=3.6

#### With _pyenv_
```
$ pyenv virtualenv myenv
$ pyenv activate myenv
```

#### With _virtualenvwrapper_
This depends on the location/name of your Python 3 executable. For example, on __Windows__:  
```$ mkvirtualenv --python=C:/python36/python.exe myenv```  
or, on __MacOS__:  
```$ mkvirtualenv --python=python3 myenv```  
  
Check the location of your Python installation with ```where```:  
```
$ where python
C:\Python27\python.exe
C:\Python36\python.exe
C:\Python38\python.exe
```
#### With pure Python (3+)
Create a virtual environment with Python 3's built-in `venv` library.  
```
$ python -m venv ~/.virtualenvs/myenv
```
Activate with 
```$ myenv\Scripts\activate.bat``` (Windows)
or ```$ source myenv/bin/activate``` (MacOS).

### Download the source code for this package.
```$ git clone https://github.com/MBKEngineers/collect.git```

### Install _collect_ as a Python package available for local use.
```
$ cd collect
$ python setup.py develop
```
or with _pip_:
```
$ cd collect
$ pip install -e .
```

### Configure package variables
Add username and password credentials to a `.env` file to enable downloading data from password-protected sources.

### Install additional package requirements (pip or pip3)
To build _collect_ documentation, Sphinx is used.  Install Sphinx and additional requirements for building documentation stored in the requirements file.
```$ python -m pip install -r requirements.txt```  
  
If pip3 comes packaged with your Python installation, another option is to use the ```pip3``` shortcut:  
```$ pip3 install -r requirements.txt```  

### Namespace
Note, there is one other Python package on PyPi named  `collect`.  However, it is not maintained and is dated 2011, so not expecting MBK codebase to use that tool.

### Adding new modules
`collect` now includes a command line interface for starting a new module called `collect-start`. Initialize a new module from a template with
```$ collect-start modulename
```
