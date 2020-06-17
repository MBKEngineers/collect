# collect
Web scraping utilties for DWR, USACE, USGS, CNRFC, CVO and SacALERT data repositories.

## Setup instructions
### Create a virtual environment, specifying Python version >=3.6
This depends on the location/name of your Python 3 executable. For example, on __Windows__:  
```> mkvirtualenv --python=C:/python36/python.exe myenv```  
or, on __MacOS__:  
```> mkvirtualenv --python=python3 myenv```  
  
Check the location of your Python installation with ```where```:  
```>
> where python
C:\Python27\python.exe
C:\Python36\python.exe
C:\Python38\python.exe
```

### Install the package requirements (pip or pip3)
```> python -m pip install -r requirements.txt```  
  
If pip3 comes packaged with your Python installation, another option is to use the ```pip3``` shortcut:  
```> pip3 install -r requirements.txt```  

### Download the source code for this package.
```> git clone https://github.com/MBKEngineers/collect.git```

### Install _collect_ as a Python package available for local use.
```> python collect/setup.py develop```

### Configure package variables
Add username and password credentials to a `.env` file to enable downloading data from password-protected sources.

### Namespace
Note, there is one other Python package on PyPi named  `collect`.  However, it is not maintained and is dated 2011, so not expecting MBK codebase to use that tool.