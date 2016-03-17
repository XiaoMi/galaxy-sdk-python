import string
from unicodedata import category
from emq.common.ttypes import GalaxyEmqServiceException
from emq.range.constants import GALAXY_EMQ_QUEUE_DELAY_SECONDS_MINIMAL, GALAXY_EMQ_QUEUE_DELAY_SECONDS_MAXIMAL, \
  GALAXY_EMQ_QUEUE_INVISIBILITY_SECONDS_MAXIMAL, GALAXY_EMQ_QUEUE_INVISIBILITY_SECONDS_MINIMAL, \
  GALAXY_EMQ_QUEUE_RECEIVE_WAIT_SECONDS_MINIMAL, GALAXY_EMQ_QUEUE_RECEIVE_WAIT_SECONDS_MAXIMAL, \
  GALAXY_EMQ_QUEUE_RECEIVE_NUMBER_MAXIMAL, GALAXY_EMQ_QUEUE_RECEIVE_NUMBER_MINIMAL, \
  GALAXY_EMQ_QUEUE_RETENTION_SECONDS_MINIMAL, GALAXY_EMQ_QUEUE_RETENTION_SECONDS_MAXIMAL, \
  GALAXY_EMQ_QUEUE_MAX_MESSAGE_BYTES_MINIMAL, GALAXY_EMQ_QUEUE_MAX_MESSAGE_BYTES_MAXIMAL, \
  GALAXY_EMQ_QUEUE_PARTITION_NUMBER_MINIMAL, GALAXY_EMQ_QUEUE_PARTITION_NUMBER_MAXIMAL, \
  GALAXY_EMQ_MESSAGE_DELAY_SECONDS_MAXIMAL, GALAXY_EMQ_MESSAGE_DELAY_SECONDS_MINIMAL, \
  GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MAXIMAL, GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MINIMAL, \
  GALAXY_EMQ_QUEUE_WRITE_QPS_MINIMAL, GALAXY_EMQ_QUEUE_WRITE_QPS_MAXIMAL, GALAXY_EMQ_QUEUE_READ_QPS_MINIMAL, \
  GALAXY_EMQ_QUEUE_READ_QPS_MAXIMAL, GALAXY_EMQ_QUEUE_MAX_SPACE_QUOTA_MINIMAL, GALAXY_EMQ_QUEUE_MAX_SPACE_QUOTA_MAXIMAL, \
  GALAXY_EMQ_QUEUE_REDRIVE_POLICY_MAX_RECEIVE_TIME_MINIMAL, GALAXY_EMQ_QUEUE_REDRIVE_POLICY_MAX_RECEIVE_TIME_MAXIMAL
from emq.message.ttypes import SendMessageRequest, ReceiveMessageRequest, ChangeMessageVisibilityRequest, \
  DeleteMessageRequest, SendMessageBatchRequest, SendMessageBatchRequestEntry, ChangeMessageVisibilityBatchRequestEntry, \
  ChangeMessageVisibilityBatchRequest, DeleteMessageBatchRequest, DeadMessageBatchRequest, DeadMessageRequest
from emq.queue.ttypes import CreateQueueRequest, ListQueueRequest, SetQueueAttributesRequest, SetPermissionRequest, \
  RevokePermissionRequest, QueryPermissionForIdRequest, SetQueueQuotaRequest, QueueQuota, CreateTagRequest, \
  DeleteTagRequest, GetTagInfoRequest, ListTagRequest, SetQueueRedrivePolicyRequest, RemoveQueueRedrivePolicyRequest, \
  ListDeadLetterSourceQueuesRequest
from emq.statistics.ttypes import SetUserQuotaRequest, GetUserQuotaRequest, GetUserUsedQuotaRequest, SetUserInfoRequest, \
  GetUserInfoRequest


class RequestChecker(object):
  def __init__(self, args=tuple()):
    self.__args = args

  def check_arg(self):
    if len(self.__args) > 1:
      errMsg = "Unknown request"
      raise GalaxyEmqServiceException(errMsg=errMsg)
    else:
      self.check_request_params(self.__args[0])

  def check_request_params(self, request):
    if isinstance(request, ListQueueRequest):
      self.validate_queue_prefix(request.queueNamePrefix)
    elif isinstance(request, CreateQueueRequest):
      self.validate_queue_name(request.queueName, False)
      self.validate_queue_attribute(request.queueAttribute)
      self.validate_queue_quota(request.queueQuota)
    elif isinstance(request, SetQueueAttributesRequest):
      self.validate_queue_name(request.queueName)
      self.validate_queue_attribute(request.queueAttribute)
    elif isinstance(request, SetQueueQuotaRequest):
      self.validate_queue_name(request.queueName)
      self.validate_queue_quota(request.queueQuota)
    elif isinstance(request, SetQueueRedrivePolicyRequest):
      self.validate_queue_name(request.queueName)
      self.validate_redrivePolcy(request.redrivePolicy)
    elif isinstance(request, RemoveQueueRedrivePolicyRequest):
      self.validate_queue_name(request.queueName)
    elif isinstance(request, ListDeadLetterSourceQueuesRequest):
      self.validate_queue_name(request.dlqName)
    elif isinstance(request, SetPermissionRequest):
      self.validate_queue_name(request.queueName)
      self.validate_not_none(request.developerId, "developerId")
      self.validate_not_none(request.permission, "permission")
    elif isinstance(request, RevokePermissionRequest):
      self.validate_queue_name(request.queueName)
      self.validate_not_none(request.developerId, "developerId")
    elif isinstance(request, QueryPermissionForIdRequest):
      self.validate_queue_name(request.queueName)
      self.validate_not_none(request.developerId, "developerId")
    elif isinstance(request, CreateTagRequest):
      self.validate_queue_name(request.queueName)
      self.validate_queue_name(request.tagName, False, False, "tag name")
      if request.attributeName:
        self.validate_not_empty(request.attributeName, "attributeName")
        self.check_message_attribute(request.attributeValue, True)
      if request.userAttributes:
        self.validate_user_attribute(request.userAttributes)
      if request.readQPSQuota:
        self.validate_readQps(request.readQPSQuota)
    elif isinstance(request, DeleteTagRequest):
      self.validate_queue_name(request.queueName)
      self.validate_queue_name(request.tagName, False, False, "tag name")
    elif isinstance(request, GetTagInfoRequest):
      self.validate_queue_name(request.queueName)
      if request.tagName is not None:
        self.validate_queue_name(request.tagName, False, False, "tag name")
    elif isinstance(request, ListTagRequest):
      self.validate_queue_name(request.queueName)
    elif isinstance(request, SendMessageRequest):
      self.validate_queue_name(request.queueName)
      self.validate_not_none(request.messageBody, "messageBody")
      if request.messageAttributes is not None:
        for attribute in request.messageAttributes.values():
          self.check_message_attribute(attribute, False)
      if request.delaySeconds is not None:
        self.validate_delaySeconds(request.delaySeconds)
      if request.invisibilitySeconds is not None:
        self.validate_invisibilitySeconds(request.invisibilitySeconds)
    elif isinstance(request, ReceiveMessageRequest):
      self.validate_queue_name(request.queueName)
      if request.maxReceiveMessageNumber is not None:
        self.validate_receiveMessageMaximumNumber(request.maxReceiveMessageNumber)
      if request.maxReceiveMessageWaitSeconds is not None:
        self.validate_receiveMessageWaitSeconds(request.maxReceiveMessageWaitSeconds)
      if request.attributeName is not None:
        self.validate_not_empty(request.attributeName, "attributeName")
        self.check_message_attribute(request.attributeValue, True)
      if request.tagName is not None:
        self.validate_queue_name(request.tagName, False, False, "tag name")
    elif isinstance(request, ChangeMessageVisibilityRequest):
      self.validate_queue_name(request.queueName)
      if request.invisibilitySeconds is not None:
        self.validate_changeInvisibilitySeconds(request.invisibilitySeconds)
      self.validate_not_none(request.receiptHandle, "receiptHandle")
      self.validate_not_empty(request.receiptHandle, "receiptHandle")
    elif isinstance(request, DeleteMessageRequest):
      self.validate_queue_name(request.queueName)
      self.validate_not_none(request.receiptHandle, "receiptHandle")
      self.validate_not_empty(request.receiptHandle, "receiptHandle")
    elif isinstance(request, DeadMessageRequest):
      self.validate_queue_name(request.queueName)
      self.validate_not_none(request.receiptHandle, "receiptHandle")
      self.validate_not_empty(request.receiptHandle, "receiptHandle")
    elif isinstance(request, SendMessageBatchRequest):
      self.validate_queue_name(request.queueName)
      sendMessageBatchRequestEntryList = request.sendMessageBatchRequestEntryList
      self.validate_not_empty(sendMessageBatchRequestEntryList, "sendMessageBatchRequestEntryList")
      entry_id_list = []
      for k in sendMessageBatchRequestEntryList:
        self.validate_not_none(k.entryId, "entryId")
        self.validate_not_empty(k.entryId, "entryId")
        entry_id_list.append(k.entryId)
        self.check_send_entry(k)
      self.check_list_duplicate(entry_id_list, "entryId")
    elif isinstance(request, ChangeMessageVisibilityBatchRequest):
      self.validate_queue_name(request.queueName)
      changeMessageVisibilityBatchRequestEntryList = request.changeMessageVisibilityRequestEntryList
      self.validate_not_empty(changeMessageVisibilityBatchRequestEntryList,
                              "changeMessageVisibilityBatchRequestEntryList")
      receipt_handle_list = []
      for k in changeMessageVisibilityBatchRequestEntryList:
        self.validate_not_none(k.receiptHandle, "receiptHandle")
        self.validate_not_empty(k.receiptHandle, "receiptHandle")
        receipt_handle_list.append(k.receiptHandle)
        self.check_change_entry(k)
      self.check_list_duplicate(receipt_handle_list, "receiptHandle")
    elif isinstance(request, DeleteMessageBatchRequest):
      self.validate_queue_name(request.queueName)
      deleteMessageBatchRequestEntryList = request.deleteMessageBatchRequestEntryList
      self.validate_not_empty(deleteMessageBatchRequestEntryList, "deleteMessageBatchRequestEntryList")
      receipt_handle_list = []
      for k in deleteMessageBatchRequestEntryList:
        self.validate_not_none(k.receiptHandle, "receiptHandle")
        self.validate_not_empty(k.receiptHandle, "receiptHandle")
        receipt_handle_list.append(k.receiptHandle)
      self.check_list_duplicate(receipt_handle_list, "receiptHandle")
    elif isinstance(request, DeadMessageBatchRequest):
      self.validate_queue_name(request.queueName)
      deadMessageBatchRequestEntryList = request.deadMessageBatchRequestEntryList
      self.validate_not_empty(deadMessageBatchRequestEntryList, "deadMessageBatchRequestEntryList")
      receipt_handle_list = []
      for k in deadMessageBatchRequestEntryList:
        self.validate_not_none(k.receiptHandle, "receiptHandle")
        self.validate_not_empty(k.receiptHandle, "receiptHandle")
        receipt_handle_list.append(k.receiptHandle)
      self.check_list_duplicate(receipt_handle_list, "receiptHandle")
    elif (isinstance(request, SetUserQuotaRequest) or isinstance(request, GetUserQuotaRequest)
          or isinstance(request, GetUserUsedQuotaRequest) or isinstance(request, SetUserInfoRequest)
          or isinstance(request, GetUserInfoRequest)):
      pass
    else:
      self.validate_queue_name(request.queueName)

  def check_send_entry(self, send_entry):
    self.validate_not_none(send_entry.messageBody, "messageBody")
    if send_entry.delaySeconds is not None:
      self.validate_delaySeconds(send_entry.delaySeconds)
    if send_entry.invisibilitySeconds is not None:
      self.validate_invisibilitySeconds(send_entry.invisibilitySeconds)
    if send_entry.messageAttributes is not None:
      for attribute in send_entry.messageAttributes.values():
        self.check_message_attribute(attribute, False)

  def check_message_attribute(self, attribute, allow_empty):
    self.validate_not_none(attribute, "messageAttribute")
    if attribute.type.lower().startswith("string"):
      if attribute.stringValue is None:
        raise GalaxyEmqServiceException(errMsg="stringValue cannot be None when type is STRING")
    elif attribute.type.lower().startswith("binary"):
      if attribute.binaryValue is None:
        raise GalaxyEmqServiceException(errMsg="binaryValue cannot be None when type is BINARY")
    elif allow_empty and attribute.type.lower() == "empty":
      return
    else:
      raise GalaxyEmqServiceException(errMsg="Attribute type must start with \"STRING\" or \"BINARY\"")
    for c in attribute.type:
      if not c in string.ascii_letters and not c in string.digits and c != '.':
        raise GalaxyEmqServiceException(errMsg="Invalid character \'" + c + "\' in attribute type")

  def check_change_entry(self, change_entry):
    self.validate_not_none(change_entry.invisibilitySeconds, "invisibilitySeconds")
    self.validate_changeInvisibilitySeconds(change_entry.invisibilitySeconds)

  def check_list_duplicate(self, l, name):
    if len(l) != len({}.fromkeys(l).keys()):
      raise GalaxyEmqServiceException(errMsg="Bad request, %s shouldn't be duplicate." % name)

  def validate_queue_name(self, queue_name, allow_slash=True, is_prefix=False, param_name="queue name"):
    chars = list(queue_name)
    if (queue_name == "" or queue_name is None) and not is_prefix:
      raise GalaxyEmqServiceException(errMsg="Bad request, %s shouldn't be empty." % param_name)
    for c in chars:
      if not self.isJavaIdentifierPart(c) or (not allow_slash and c == "/"):
        raise GalaxyEmqServiceException(errMsg="Bad request, Invalid characters in %s." % param_name)
    if allow_slash and len(queue_name.split("/")) != 2 and not is_prefix:
      raise GalaxyEmqServiceException(errMsg="Bad request, please check your '/' in %s." % param_name)
    if is_prefix and len(queue_name.split("/")) != 1 and len(queue_name.split("/")) != 2:
      raise GalaxyEmqServiceException(errMsg="Bad request, please check your '/' in %s." % param_name)

  @staticmethod
  def isJavaIdentifierPart(c):
    if c in string.ascii_letters:
      return True
    if c in string.digits:
      return True
    if c in string.punctuation:
      return True
    if category(unicode(c)) == 'Sc':
      return True
    if category(unicode(c)) == 'Mn':
      return True
    if category(unicode(c)) == 'N1':
      return True
    if category(unicode(c)) == 'Mc':
      return False
    return False

  def validate_queue_prefix(self, queue_prefix):
    self.validate_queue_name(queue_prefix, True, True, "queue name prefix")

  def validate_queue_attribute(self, queue_attribute):
    if not queue_attribute:
      return
    if queue_attribute.delaySeconds:
      self.validate_delaySeconds(queue_attribute.delaySeconds)
    if queue_attribute.invisibilitySeconds:
      self.validate_invisibilitySeconds(queue_attribute.invisibilitySeconds)
    if queue_attribute.receiveMessageWaitSeconds:
      self.validate_receiveMessageWaitSeconds(queue_attribute.receiveMessageWaitSeconds)
    if queue_attribute.receiveMessageMaximumNumber:
      self.validate_receiveMessageMaximumNumber(queue_attribute.receiveMessageMaximumNumber)
    if queue_attribute.messageRetentionSeconds:
      self.validate_messageRetentionSeconds(queue_attribute.messageRetentionSeconds)
    if queue_attribute.messageMaximumBytes:
      self.validate_messageMaximumBytes(queue_attribute.messageMaximumBytes)
    if queue_attribute.partitionNumber:
      self.validate_partitionNumber(queue_attribute.partitionNumber)
    if queue_attribute.userAttributes:
      self.validate_user_attribute(queue_attribute.userAttributes)

  def validate_user_attribute(self, attribute):
    for key, value in attribute.items():
      self.validate_not_empty(key, "user attribute name")
      self.validate_not_empty(value, "user attribute value for \"" + key + "\"")

  def validate_queue_quota(self, queue_quota):
    if not queue_quota:
      return
    if not queue_quota.throughput:
      return
    if queue_quota.throughput.readQps:
      self.validate_readQps(queue_quota.throughput.readQps)
    if queue_quota.throughput.writeQps:
      self.validate_writeQps(queue_quota.throughput.writeQps)

  def check_filed_range(self, value, low, high, name):
    if not isinstance(value, int):
      raise GalaxyEmqServiceException(errMsg="Bad request, wrong date type of %s!" % name)
    if value < low or value > high:
      raise GalaxyEmqServiceException(errMsg="Bad request, the attribute value of %s is out of range!" % name)

  def validate_delaySeconds(self, delaySeconds):
    self.check_filed_range(delaySeconds,
                           GALAXY_EMQ_QUEUE_DELAY_SECONDS_MINIMAL,
                           GALAXY_EMQ_QUEUE_DELAY_SECONDS_MAXIMAL,
                           "delaySeconds")

  def validate_invisibilitySeconds(self, invisibilitySeconds):
    self.check_filed_range(invisibilitySeconds,
                           GALAXY_EMQ_QUEUE_INVISIBILITY_SECONDS_MINIMAL,
                           GALAXY_EMQ_QUEUE_INVISIBILITY_SECONDS_MAXIMAL,
                           "invisibilitySeconds")

  def validate_changeInvisibilitySeconds(self, invisibilitySeconds):
    self.check_filed_range(invisibilitySeconds, 0,
                           GALAXY_EMQ_QUEUE_INVISIBILITY_SECONDS_MAXIMAL,
                           "invisibilitySeconds")

  def validate_receiveMessageWaitSeconds(self, receiveMessageWaitSeconds):
    self.check_filed_range(receiveMessageWaitSeconds,
                           GALAXY_EMQ_QUEUE_RECEIVE_WAIT_SECONDS_MINIMAL,
                           GALAXY_EMQ_QUEUE_RECEIVE_WAIT_SECONDS_MAXIMAL,
                           "receiveMessageWaitSeconds")

  def validate_receiveMessageMaximumNumber(self, receiveMessageMaximumNumber):
    self.check_filed_range(receiveMessageMaximumNumber,
                           GALAXY_EMQ_QUEUE_RECEIVE_NUMBER_MINIMAL,
                           GALAXY_EMQ_QUEUE_RECEIVE_NUMBER_MAXIMAL,
                           "receiveMessageMaximumNumber")

  def validate_messageRetentionSeconds(self, messageRetentionSeconds):
    self.check_filed_range(messageRetentionSeconds,
                           GALAXY_EMQ_QUEUE_RETENTION_SECONDS_MINIMAL,
                           GALAXY_EMQ_QUEUE_RETENTION_SECONDS_MAXIMAL,
                           "messageRetentionSeconds")

  def validate_messageMaximumBytes(self, messageMaximumBytes):
    self.check_filed_range(messageMaximumBytes,
                           GALAXY_EMQ_QUEUE_MAX_MESSAGE_BYTES_MINIMAL,
                           GALAXY_EMQ_QUEUE_MAX_MESSAGE_BYTES_MAXIMAL,
                           "messageMaximumBytes")

  def validate_partitionNumber(self, partitionNumber):
    self.check_filed_range(partitionNumber,
                           GALAXY_EMQ_QUEUE_PARTITION_NUMBER_MINIMAL,
                           GALAXY_EMQ_QUEUE_PARTITION_NUMBER_MAXIMAL,
                           "partitionNumber")

  def validate_messageDelaySeconds(self, messageDelaySeconds):
    self.check_filed_range(messageDelaySeconds,
                           GALAXY_EMQ_MESSAGE_DELAY_SECONDS_MINIMAL,
                           GALAXY_EMQ_MESSAGE_DELAY_SECONDS_MAXIMAL,
                           "messageDelaySeconds")

  def validate_messageInvisibilitySeconds(self, messageInvisibilitySeconds):
    self.check_filed_range(messageInvisibilitySeconds,
                           GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MINIMAL,
                           GALAXY_EMQ_MESSAGE_INVISIBILITY_SECONDS_MAXIMAL,
                           "messageInvisibilitySeconds")

  def validate_spaceQuota(self, spaceQuota):
    self.check_filed_range(spaceQuota,
                           GALAXY_EMQ_QUEUE_MAX_SPACE_QUOTA_MINIMAL,
                           GALAXY_EMQ_QUEUE_MAX_SPACE_QUOTA_MAXIMAL,
                           "spaceQuota")

  def validate_readQps(self, readQps):
    self.check_filed_range(readQps,
                           GALAXY_EMQ_QUEUE_READ_QPS_MINIMAL,
                           GALAXY_EMQ_QUEUE_READ_QPS_MAXIMAL,
                           "readQps")

  def validate_writeQps(self, writeQps):
    self.check_filed_range(writeQps,
                           GALAXY_EMQ_QUEUE_WRITE_QPS_MINIMAL,
                           GALAXY_EMQ_QUEUE_WRITE_QPS_MAXIMAL,
                           "writeQps")

  def validate_redrivePolcy(self, redrivePolicy):
    self.validate_queue_name(redrivePolicy.dlqName)
    self.check_filed_range(redrivePolicy.maxReceiveTime,
                           GALAXY_EMQ_QUEUE_REDRIVE_POLICY_MAX_RECEIVE_TIME_MINIMAL,
                           GALAXY_EMQ_QUEUE_REDRIVE_POLICY_MAX_RECEIVE_TIME_MAXIMAL)

  def validate_not_none(self, param, name):
    if param is None:
      raise GalaxyEmqServiceException(errMsg="Bad request, the %s is required!" % name)

  def validate_not_empty(self, param, name):
    if not param:
      raise GalaxyEmqServiceException(errMsg="Bad request, the %s shouldn't be empty!" % name)

