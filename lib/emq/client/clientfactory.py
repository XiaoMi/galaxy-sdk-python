# encoding: utf-8
import os
import sys
import platform
import time
import socket
import uuid
from emq.client.constants import THRIFT_PROTOCOL_MAP, DEFAULT_SECURE_SERVICE_ENDPOINT, QUEUE_SERVICE_PATH, \
  DEFAULT_CLIENT_TIMEOUT, DEFAULT_CLIENT_CONN_TIMEOUT, MESSAGE_SERVICE_PATH, STATISTICS_SERVICE_PATH
from emq.client.requestchecker import RequestChecker
from emq.client.thttpclient import THttpClient
from emq.common.constants import ERROR_BACKOFF, MAX_RETRY, ERROR_RETRY_TYPE
from emq.common.ttypes import Version, GalaxyEmqServiceException
from emq.message import MessageService
from emq.queue import QueueService
from emq.statistics import StatisticsService
from rpc.common.ttypes import ThriftProtocol


class ClientFactory:
  def __init__(self, credential, thrift_protocol=ThriftProtocol.TBINARY):
    self._credential = credential
    self._protocol = thrift_protocol
    ver = Version()
    version = "%s.%s" % (ver.major, ver.minor)
    script = os.path.basename(sys.argv[0])
    if script:
      self._agent = "Python-SDK/%s Python/%s(%s)" % \
                    (version, platform.python_version(), script)
    else:
      self._agent = "Python-SDK/%s Python/%s" % (version, platform.python_version())

  def queue_client(self, endpoint=DEFAULT_SECURE_SERVICE_ENDPOINT,
                   timeout=DEFAULT_CLIENT_TIMEOUT, is_retry=False, max_retry=MAX_RETRY):
    url = endpoint + QUEUE_SERVICE_PATH
    client = self.get_client(QueueService.Client, url, timeout)
    return RetryableClient(client, is_retry, max_retry)

  def message_client(self, endpoint=DEFAULT_SECURE_SERVICE_ENDPOINT,
                     timeout=DEFAULT_CLIENT_CONN_TIMEOUT, is_retry=False, max_retry=MAX_RETRY):
    url = endpoint + MESSAGE_SERVICE_PATH
    client = self.get_client(MessageService.Client, url, timeout)
    return RetryableClient(client, is_retry, max_retry)

  def statistics_client(self, endpoint=DEFAULT_SECURE_SERVICE_ENDPOINT,
                        timeout=DEFAULT_CLIENT_CONN_TIMEOUT, is_retry=False, max_retry=MAX_RETRY):
    url = endpoint + STATISTICS_SERVICE_PATH
    client = self.get_client(StatisticsService.Client, url, timeout)
    return RetryableClient(client, is_retry, max_retry)

  def get_client(self, clazz, url, timeout):
    return ThreadSafeClient(clazz, self._credential, url, timeout, self._agent, self._protocol)


class RetryableClient:
  def __init__(self, client, is_retry, max_retry):
    self.client = client
    self.is_retry = is_retry
    self.max_retry = max_retry

  def __getattr__(self, item):
    def __call_with_retries(*args):
      retry = 0
      while retry < self.max_retry:
        try:
          return getattr(self.client, item)(*args)
        except GalaxyEmqServiceException, ex:
          error_type = self.__get_error_retry_type(ex.errorCode, item)
          if error_type == 0 or (self.is_retry and error_type == 1):
            sec = ERROR_BACKOFF.get(ex.errorCode, 0) / 1000.0 * (1 << retry)
            time.sleep(sec)
            retry += 1
            # print "retry, time is:%d" % retry
          else:
            # print "won't retry, error code is:%s" % ex
            raise ex
        except socket.timeout, se:
          if self.is_retry and retry < self.max_retry:
            retry += 1
          else:
            raise se
        except socket.error:
          break

    return __call_with_retries

  def __get_error_retry_type(self, errorCode, item):
    retry_type = ERROR_RETRY_TYPE.get(errorCode, -1)
    if retry_type == 2 and (item.startswith("deleteMessage") or item.startswith("changeMessage")):
      return 0
    elif retry_type == 2:
      return 1
    else:
      return retry_type


class ThreadSafeClient:
  def __init__(self, clazz, credential, url, timeout, agent, thrift_protocol):
    self.clazz = clazz
    self.credential = credential
    self.url = url
    self.timeout = timeout
    self.agent = agent
    self.protocol = thrift_protocol

  def __getattr__(self, item):
    def __call_with_new_client(*args):
      request_checker = RequestChecker(args)
      request_checker.check_arg()
      requestId = self.generateRandomId(8)
      uri = '%s?id=%s&type=%s' % (self.url, requestId, item)
      http_client = THttpClient(self.credential, uri, self.timeout, self.protocol)
      http_client.setCustomHeaders({'User-Agent': self.agent})

      protocol_class = THRIFT_PROTOCOL_MAP[self.protocol]
      protocol_module = 'thrift.protocol.' + protocol_class
      mod = __import__(protocol_module, fromlist=[protocol_class])
      protocol_instance = getattr(mod, protocol_class)(http_client)
      client = self.clazz(protocol_instance, protocol_instance)
      return getattr(client, item)(*args)

    return __call_with_new_client

  def generateRandomId(self, length):
    requestId = str(uuid.uuid4())
    return requestId[0:length]