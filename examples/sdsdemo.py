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
from sds.table.ttypes import KeySpec, EntityGroupSpec, LocalSecondaryIndexSpec, SecondaryIndexConsistencyMode
from sds.table.ttypes import TableSchema
from sds.table.ttypes import TableQuota
from sds.table.ttypes import TableMetadata
from sds.table.ttypes import TableSpec
from sds.table.ttypes import ProvisionThroughput
from sds.table.ttypes import PutRequest
from sds.table.ttypes import GetRequest
from sds.table.ttypes import ScanRequest


# change default encodings if unicode is used
reload(sys)
sys.setdefaultencoding('utf-8')

endpoint = "http://cnbj-s0.sds.api.xiaomi.com"
# Set your AppKey and AppSecret
appKey = ""
appSecret = ""
credential = Credential(UserType.APP_SECRET, appKey, appSecret)
client_factory = ClientFactory(credential, True)
# Clients are not thread-safe
admin_client = client_factory.new_admin_client(endpoint + ADMIN_SERVICE_PATH,
                                               DEFAULT_ADMIN_CLIENT_TIMEOUT)
table_client = client_factory.new_table_client(endpoint + TABLE_SERVICE_PATH,
                                               DEFAULT_CLIENT_TIMEOUT)

table_spec = TableSpec(
    schema=TableSchema(entityGroup=EntityGroupSpec(attributes=[KeySpec(attribute='userId', asc=True)], enableHash=True),
                       primaryIndex=[KeySpec(attribute='noteId', asc=False)],
                       secondaryIndexes={
                           'mtime_index': LocalSecondaryIndexSpec(indexSchema=[KeySpec(attribute='mtime', asc=False)],
                                                                  projections=['title', 'noteId'],
                                                                  consistencyMode=SecondaryIndexConsistencyMode.EAGER),
                           'category_index': LocalSecondaryIndexSpec(indexSchema=[KeySpec(attribute='category')],
                                                          consistencyMode=SecondaryIndexConsistencyMode.LAZY)},
                       attributes={
                           'userId': DataType.STRING,
                           'noteId': DataType.INT64,
                           'title': DataType.STRING,
                           'content': DataType.STRING,
                           'version': DataType.INT64,
                           'mtime': DataType.INT64,
                           'category': DataType.STRING_SET
                       }),
    metadata=TableMetadata(quota=TableQuota(size=100 * 1024 * 1024),
                           throughput=ProvisionThroughput(readCapacity=20, writeCapacity=20)))

table_name = "python-test-note"
categories = ['work', 'travel', 'food']
M = 20
try:
    admin_client.dropTable(table_name)
except ServiceException, se:
    assert se.errorCode == ErrorCode.RESOURCE_NOT_FOUND, "Unexpected error: %s" % se.errorCode

admin_client.createTable(table_name, table_spec)

for i in range(0, M):
    version = 0
    put = PutRequest(tableName=table_name,
                     record={
                         'userId': datum('user1'),
                         'noteId': datum(i, DataType.INT64),
                         'title': datum('Title' + str(i)),
                         'content': datum('note ' + str(i)),
                         'version': datum(version, DataType.INT64),
                         'mtime': datum(i * i % 10, DataType.INT64),
                         'category': datum([categories[i % len(categories)], categories[(i + 1) % len(categories)]],
                                           DataType.STRING_SET)
                     })
    table_client.put(put)
    print "put record %d" % i

# random access
print "================= get note by id ===================="
get = GetRequest(tableName=table_name,
                 keys={
                     'userId': datum('user1'),
                     'noteId': datum(random.randint(0, M), DataType.INT64)
                 },
                 attributes=['title', 'content'])
print "get record: %s" % values(table_client.get(get).item)

# scan by last modify time
print "================= scan by last modify time ===================="
start_key = {'userId': datum('user1')}
stop_key = start_key
scan = ScanRequest(tableName=table_name,
                   indexName='mtime_index',
                   startKey=start_key,  # None or unspecified means begin of the table
                   stopKey=stop_key,  # None or unspecified means end of the table
                   attributes=['noteId', 'title', 'mtime'],  # scan all attributes if not specified
                   limit=M)  # batch size

for record in scan_iter(table_client, scan):
    print record

# get noteId which contain category food
print "================= get notes which contain category food ===================="
start_key = {'userId': datum('user1'), 'category': datum('food')}
stop_key = start_key
scan = ScanRequest(tableName=table_name,
                   indexName='category_index',
                   startKey=start_key,  # None or unspecified means begin of the table
                   stopKey=stop_key,  # None or unspecified means end of the table
                   attributes=['noteId', 'category'],  # scan all attributes if not specified
                   limit=M)  # batch size
for record in scan_iter(table_client, scan):
    print record

admin_client.dropTable(table_name)

