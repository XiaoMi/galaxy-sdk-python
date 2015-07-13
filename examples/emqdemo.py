from time import sleep
from emq.client.clientfactory import ClientFactory
from emq.common.ttypes import GalaxyEmqServiceException
from emq.message.ttypes import SendMessageRequest, ReceiveMessageRequest, ChangeMessageVisibilityRequest
from emq.queue.ttypes import QueueAttribute, CreateQueueRequest, ListQueueRequest, DeleteQueueRequest
from rpc.auth.ttypes import Credential, UserType


# Please set your AppKey and AppSecret
app_key = ""
app_secret = ""

endpoint = ""
credential = Credential(UserType.APP_SECRET, app_key, app_secret)
client_factory = ClientFactory(credential)
queue_client = client_factory.queue_client(endpoint)
message_client = client_factory.message_client(endpoint)
queue_name = "testPythonExampleQueue"

queue_attribute = QueueAttribute()
create_request = CreateQueueRequest(queueName=queue_name, queueAttribute=queue_attribute)

create_queue_response = queue_client.createQueue(create_request)
queue_name = create_queue_response.queueName
print "created queue:" + queue_name
listqueue_equest = ListQueueRequest(queueNamePrefix="test")
print queue_client.listQueue(listqueue_equest)

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

visibility_seconds = 200
change_message_visibility_request = ChangeMessageVisibilityRequest(queue_name, receipt_handle, visibility_seconds)
message_client.changeMessageVisibilitySeconds(change_message_visibility_request)

delete_request = DeleteQueueRequest(queueName=queue_name)
try:
  queue_client.deleteQueue(delete_request)
  print "deleted queue:" + queue_name
except GalaxyEmqServiceException, e:
  print e



