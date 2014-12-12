# -*- coding: utf-8 -*-
import random
import time
import sys

from sds.client.clientfactory import ClientFactory
from sds.client.datumutil import datum
from sds.client.datumutil import values
from sds.client.tablescanner import scan_iter
from sds.auth.ttypes import Credential
from sds.auth.ttypes import UserType
from sds.common.constants import ADMIN_SERVICE_PATH
from sds.common.constants import TABLE_SERVICE_PATH
from sds.common.constants import DEFAULT_ADMIN_CLIENT_TIMEOUT
from sds.common.constants import DEFAULT_CLIENT_TIMEOUT
from sds.errors.ttypes import ServiceException
from sds.errors.constants import ErrorCode
from sds.table.constants import DataType
from sds.table.ttypes import KeySpec
from sds.table.ttypes import TableSchema
from sds.table.ttypes import TableQuota
from sds.table.ttypes import TableMetadata
from sds.table.ttypes import TableSpec
from sds.table.ttypes import ProvisionThroughput
from sds.table.ttypes import PutRequest
from sds.table.ttypes import GetRequest
from sds.table.ttypes import ScanRequest
from sds.table.ttypes import BatchRequestItem
from sds.table.ttypes import BatchRequest
from sds.table.ttypes import BatchOp
from sds.table.ttypes import Request


# change default encodings if unicode is used
reload(sys)
sys.setdefaultencoding('utf-8')

endpoint = "http://sds.api.xiaomi.com"
# Set yout AppKey and AppSecret
appKey = ""
appSecret = ""
credential = Credential(UserType.APP_SECRET, appKey, appSecret)
client_factory = ClientFactory(credential, True)
# Clients are not thread-safe
admin_client = client_factory.new_admin_client(endpoint + ADMIN_SERVICE_PATH,
                                               DEFAULT_ADMIN_CLIENT_TIMEOUT)
table_client = client_factory.new_table_client(endpoint + TABLE_SERVICE_PATH,
                                               DEFAULT_CLIENT_TIMEOUT)
table_client = client_factory.new_table_client(endpoint + TABLE_SERVICE_PATH,
                                               1)

table_name = "python-test-weather"

table_spec = TableSpec(
    schema=TableSchema(primaryIndex=[KeySpec(attribute='cityId', asc=False)],
                       attributes={
                           'cityId': DataType.STRING,
                           'timestamp': DataType.INT64,
                           'score': DataType.DOUBLE,
                           'pm25': DataType.INT64
                       }),
    metadata=TableMetadata(quota=TableQuota(size=100 * 1024 * 1024),
                           throughput=ProvisionThroughput(readQps=2000, writeQps=2000)))

try:
    admin_client.dropTable(table_name)
except ServiceException, se:
    assert se.errorCode == ErrorCode.RESOURCE_NOT_FOUND, "Unexpected error: %s" % se.errorCode

admin_client.createTable(table_name, table_spec)
print admin_client.describeTable(table_name)

# put data
now = int(time.time())

cities = ["北京", "Beihai", "Dalian", "Dandong", "Fuzhou", "Guangzhou", "Haikou",
          "Hankou", "Huangpu", "Jiujiang", "Lianyungang", "Nanjing", "Nantong", "Ningbo",
          "Qingdao", "Qinhuangdao", "Rizhao", "Sanya", "Shanghai", "Shantou", "Shenzhen",
          "Tianjin", "Weihai", "Wenzhou", "Xiamen", "Yangzhou", "Yantai"]

for i in range(0, len(cities)):
    put = PutRequest(tableName=table_name,
                     record={
                         'cityId': datum(cities[i]),
                         'timestamp': datum(now, DataType.INT64),
                         'score': datum(random.random() * 100),
                         'pm25': datum(random.randint(0, 100), DataType.INT64)
                     })
    table_client.put(put)
    print "put record %d" % i

# batch put
batch_items = []
for i in range(0, len(cities)):
    put = PutRequest(tableName=table_name,
                     record={
                         'cityId': datum(cities[i]),
                         'timestamp': datum(now, DataType.INT64),
                         'score': datum(random.random() * 100),
                         'pm25': datum(random.randint(0, 100), DataType.INT64)
                     })
    batch_items.append(BatchRequestItem(action=BatchOp.PUT, request=Request(putRequest=put)))

batch = BatchRequest(batch_items)
table_client.batch(batch)

# get data
get = GetRequest(tableName=table_name,
                 keys={
                     'cityId': datum(cities[random.randint(0, len(cities) - 1)]),
                     'timestamp': datum(now, DataType.INT64)
                 },
                 attributes=['pm25'])
print "get record: %s" % values(table_client.get(get).item)

# scan table
scan = ScanRequest(tableName=table_name,
                   startKey=None,  # None or unspecified means begin of the table
                   stopKey=None,  # None or unspecified means end of the table
                   attributes=['cityId', 'score'],  # scan all attributes if not specified
                   condition='score > 0',  # condition to meet
                   limit=5)  # batch size

for record in scan_iter(table_client, scan):
    print record

print "Dropping table %s" % table_name
admin_client.dropTable(table_name)
