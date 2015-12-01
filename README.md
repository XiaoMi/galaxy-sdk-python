# Galaxy SDK for Python

This is XiaoMi Galaxy SDK for python, which allows Python developers to write software that makes use of XiaoMi Galaxy [EMR/SDS/FDS/EMQ].

## Installing

Requires Python >= 2.6. You can install using pip or easy_install:

`pip install galaxy-sdk-python` 
or `easy_install galaxy-sdk-python`

or download source codes from [https://github.com/XiaoMi/galaxy-sdk-python.git]() 
and add the 'lib' dir to your python path.

## Configuring Credential

Before using the SDK, ensure that you've configured credential. 
You can get the necessary credential by registering on http://dev.xiaomi.com/.
To configure credential, you may use codes like:

```
appKey = "MY-APP-KEY"
appSecret = "MY-SECRET-KEY"
credential = Credential(UserType.APP_SECRET, appKey, appSecret)
```

## Usage

To use SDK, you can import like:

```
from sds.table.ttypes import PutRequest
from sds.table.ttypes import GetRequest
```

We have two examples in the 'examples' dir of the source code,
users can run these examples after credential configured.

```
python examples/basic.py
```
