# encoding: utf-8
import os
import sys
import platform
import time
import socket
import uuid
from emq.client.constants import THRIFT_PROTOCOL_MAP, DEFAULT_SECURE_SERVICE_ENDPOINT, QUEUE_SERVICE_PATH, \
  DEFAULT_CLIENT_TIMEOUT, DEFAULT_CLIENT_CONN_TIMEOUT, MESSAGE_SERVICE_PATH
from emq.client.requestchecker import RequestChecker
from emq.client.thttpclient import THttpClient
from emq.common.ttypes import Version, GalaxyEmqServiceException
from emq.message import MessageService
from emq.queue import QueueService
from rpc.common.ttypes import ThriftProtocol
from rpc.errors.constants import ERROR_BACKOFF, MAX_RETRY


class ClientFactory:
  def __init__(self, credential, retry_if_timeout=False, thrift_protocol=ThriftProtocol.TBINARY):
    self._credential = credential
    self._retry_if_timeout = retry_if_timeout
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
                   timeout=DEFAULT_CLIENT_TIMEOUT):
    url = endpoint + QUEUE_SERVICE_PATH
    client = self.get_client(QueueService.Client, url, timeout)
    return RetryableClient(client, self._retry_if_timeout)

  def message_client(self, endpoint=DEFAULT_SECURE_SERVICE_ENDPOINT,
                     timeout=DEFAULT_CLIENT_CONN_TIMEOUT):
    url = endpoint + MESSAGE_SERVICE_PATH
    client = self.get_client(MessageService.Client, url, timeout)
    return RetryableClient(client, self._retry_if_timeout)

  def get_client(self, clazz, url, timeout):
    return ThreadSafeClient(clazz, self._credential, url, timeout, self._agent, self._protocol)

class RetryableClient:
  def __init__(self, client, retryIfTimeout):
    self.client = client
    self.retryIfTimeout = retryIfTimeout

  def __getattr__(self, item):
    def __call_with_retries(*args):
      retry = 0
      while retry < 3:
        try:
          return getattr(self.client, item)(*args)
        except GalaxyEmqServiceException, ex:
          if ERROR_BACKOFF.has_key(ex.errorCode) and retry < MAX_RETRY:
            sec = ERROR_BACKOFF[ex.errorCode] / 1000.0 * (1 << retry)
            time.sleep(sec)
            retry += 1
          else:
            raise ex
        except socket.timeout, se:
          if self.retryIfTimeout and retry < MAX_RETRY:
            retry += 1
          else:
            raise se

    return __call_with_retries


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