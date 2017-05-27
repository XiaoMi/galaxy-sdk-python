from pprint import pprint
from time import sleep
from emq.client.clientfactory import ClientFactory
from emq.common.ttypes import GalaxyEmqServiceException
from emq.message.ttypes import SendMessageRequest, ReceiveMessageRequest, ChangeMessageVisibilityRequest, \
  DeleteMessageBatchRequestEntry, DeleteMessageBatchRequest, ChangeMessageVisibilityBatchRequestEntry, \
  ChangeMessageVisibilityBatchRequest
from emq.queue.ttypes import QueueAttribute, CreateQueueRequest, ListQueueRequest, DeleteQueueRequest, QueueQuota,\
  Throughput, SetQueueQuotaRequest, CreateTagRequest, DeleteTagRequest
from rpc.auth.ttypes import Credential, UserType


# Please set your AppKey and AppSecret
app_key = "" # Set your AppKey, like "5521728135794"
app_secret = "" # Set your AppSecret, like "K7czwCuHttwZD49DD/qKzg==" 

print ("\n== CREATE OPERATION ==")

credential = Credential(UserType.APP_SECRET, app_key, app_secret)
client_factory = ClientFactory(credential)
queue_client = client_factory.queue_client("http://awsbj0.emq.api.xiaomi.com")
message_client = client_factory.message_client("http://awsbj0.emq.api.xiaomi.com")

queue_name = "testPythonExampleQueue"

queue_attribute = QueueAttribute()
queue_quota = QueueQuota(throughput=Throughput(readQps=100, writeQps=100))
create_request = CreateQueueRequest(queueName=queue_name, queueAttribute=queue_attribute, queueQuota=queue_quota)

create_queue_response = queue_client.createQueue(create_request)
#pprint(vars(create_queue_response))
print (create_queue_response)
# print "create queue response:" + create_queue_response
queue_name = create_queue_response.queueName
print ("created queue:" + queue_name)
list_queue_request = ListQueueRequest(queueNamePrefix="test")
print (queue_client.listQueue(list_queue_request))

tag_name = "tagTest"
create_tag_request = CreateTagRequest(queueName=queue_name, tagName=tag_name)
create_tag_response = queue_client.createTag(create_tag_request)
print ("create tag:" + queue_name + "~" + tag_name)
pprint(vars(create_tag_response))

print ("\n== SEND OPERATION ==")

message_body = "test message body"
delay_seconds = 2
send_message_request = SendMessageRequest(queue_name, message_body, delay_seconds)
send_message_response = message_client.sendMessage(send_message_request)
print ("Message Id:", send_message_response.messageID)
print ("Message Body MD5:", send_message_response.bodyMd5)
print ("Message Body:", message_body)
sleep(3)

print ("\n== RECEIVE FROM QUEUE DEFAULT ==")
receive_message_request = ReceiveMessageRequest(queue_name)
receive_message_response_list = None
while not receive_message_response_list:
  receive_message_response_list = message_client.receiveMessage(receive_message_request)

entryList = []
for receive_message in receive_message_response_list:
  print ("Message Id:", receive_message.messageID)
  print ("Message ReceiptHandle:", receive_message.receiptHandle)
  print ("Message Body:", receive_message.messageBody)
  entryList.append(DeleteMessageBatchRequestEntry(receive_message.receiptHandle))

if entryList:
  delete_message_batch_request = DeleteMessageBatchRequest(queue_name, entryList)
  message_client.deleteMessageBatch(delete_message_batch_request)

print ("\n== RECEIVE FROM QUEUE TAG ==")

receive_message_request = ReceiveMessageRequest(queueName=queue_name, tagName=tag_name)
receive_message_response_list = None
while not receive_message_response_list:
  receive_message_response_list = message_client.receiveMessage(receive_message_request)

entryList = []
for receive_message in receive_message_response_list:
  print ("Message Id:", receive_message.messageID)
  print ("Message ReceiptHandle:", receive_message.receiptHandle)
  print ("Message Body:", receive_message.messageBody)
  entryList.append(ChangeMessageVisibilityBatchRequestEntry(receive_message.receiptHandle, 0))

if entryList:
  change_message_batch_request = ChangeMessageVisibilityBatchRequest(queue_name, entryList)
  message_client.changeMessageVisibilitySecondsBatch(change_message_batch_request)

print ("\n== QUOTA OPERATION ==")

set_quota_request = SetQueueQuotaRequest(queueName=queue_name,
                                         queueQuota=QueueQuota(throughput=Throughput(readQps=200, writeQps=200)))
try:
  response = queue_client.setQueueQuota(set_quota_request)
  pprint(vars(response))
except GalaxyEmqServiceException as e:
  print (e)

print ("\n== DELETE OPERATION ==")
delete_tag_request = DeleteTagRequest(queueName=queue_name, tagName=tag_name)
queue_client.deleteTag(delete_tag_request)
print ("delete tag:" + queue_name + "~" + tag_name)

delete_request = DeleteQueueRequest(queueName=queue_name)
try:
  queue_client.deleteQueue(delete_request)
  print ("deleted queue:" + queue_name)
except GalaxyEmqServiceException as e:
  print (e)




