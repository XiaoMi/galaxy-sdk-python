from time import sleep
from emq.client.clientfactory import ClientFactory
from emq.common.ttypes import GalaxyEmqServiceException
from emq.message.ttypes import SendMessageRequest, ReceiveMessageRequest, ChangeMessageVisibilityRequest
from emq.queue.ttypes import QueueAttribute, CreateQueueRequest, ListQueueRequest, DeleteQueueRequest
from rpc.auth.ttypes import Credential, UserType


# Please set your AppKey ,AppSecret and DeveloperId.
app_key = "5341725076926"
app_secret = "vhlqXBAsWMbRIKZx+UBfPQ=="
developer_id = "88803"

endpoint = "http://lg-hadoop-open1-tst-fds02.bj:21101"
# endpoint = "http://staging.emq.api.xiaomi.com"
credential = Credential(UserType.APP_SECRET, app_key, app_secret)
client_factory = ClientFactory(credential)
queue_client = client_factory.queue_client(endpoint)
message_client = client_factory.message_client(endpoint)
queue_name = "pythonExampleQueue"

queue_attribute = QueueAttribute()
create_request = CreateQueueRequest(queueName=queue_name, queueAttribute=queue_attribute)

queue_name = "%s/%s" % (developer_id, queue_name)
delete_request = DeleteQueueRequest(queueName=queue_name)
try:
  queue_client.deleteQueue(delete_request)
except GalaxyEmqServiceException, e:
  print e
queue_client.createQueue(create_request)
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



