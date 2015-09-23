from pprint import pprint
from time import sleep
from emq.client.clientfactory import ClientFactory
from emq.common.ttypes import GalaxyEmqServiceException
from emq.message.ttypes import SendMessageRequest, ReceiveMessageRequest, ChangeMessageVisibilityRequest
from emq.queue.ttypes import QueueAttribute, CreateQueueRequest, ListQueueRequest, DeleteQueueRequest, QueueQuota, \
  SpaceQuota, Throughput, SetQueueQuotaRequest
from rpc.auth.ttypes import Credential, UserType


# Please set your AppKey and AppSecret
app_key = ""
app_secret = ""

credential = Credential(UserType.APP_SECRET, app_key, app_secret)
client_factory = ClientFactory(credential)
queue_client = client_factory.queue_client()
message_client = client_factory.message_client()
queue_name = "testPythonExampleQueue"

queue_attribute = QueueAttribute()
queue_quota = QueueQuota(throughput=Throughput(readQps=100, writeQps=100))
create_request = CreateQueueRequest(queueName=queue_name, queueAttribute=queue_attribute, queueQuota=queue_quota)

create_queue_response = queue_client.createQueue(create_request)
pprint(vars(create_queue_response))
# print "create queue response:" + create_queue_response
queue_name = create_queue_response.queueName
print "created queue:" + queue_name
list_queue_request = ListQueueRequest(queueNamePrefix="test")
print queue_client.listQueue(list_queue_request)

message_body = "test message body"
delay_seconds = 2
send_message_request = SendMessageRequest(queue_name, message_body, delay_seconds)
message_client.sendMessage(send_message_request)

sleep(3)
receive_message_request = ReceiveMessageRequest(queue_name)
receive_message_response_list = message_client.receiveMessage(receive_message_request)
receipt_handle = ""
for receive_message in receive_message_response_list:
  print receive_message.messageID
  print receive_message.receiptHandle
  receipt_handle = receive_message.receiptHandle
  print receive_message.messageBody

visibility_seconds = 0
change_message_visibility_request = ChangeMessageVisibilityRequest(queue_name, receipt_handle, visibility_seconds)
message_client.changeMessageVisibilitySeconds(change_message_visibility_request)
set_quota_request = SetQueueQuotaRequest(queueName=queue_name,
                                         queueQuota=QueueQuota(spaceQuota=SpaceQuota(size=200),
                                                               throughput=Throughput(readQps=200, writeQps=200)))
try:
  response = queue_client.setQueueQuota(set_quota_request)
  pprint(vars(response))
except GalaxyEmqServiceException, e:
  print e

delete_request = DeleteQueueRequest(queueName=queue_name)
try:
  queue_client.deleteQueue(delete_request)
  print "deleted queue:" + queue_name
except GalaxyEmqServiceException, e:
  print e




