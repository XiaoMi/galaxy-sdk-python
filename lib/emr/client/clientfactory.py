# !/usr/bin/env python
# encoding=utf-8
import os
import platform
import socket
import sys
import time
import uuid

from constants import DEFAULT_CLIENT_TIMEOUT, MAX_RETRY, THRIFT_PROTOCOL_MAP
from emr.client.constants import ERROR_BACKOFF
from emr.exceptions.GalaxyEmrServiceException import GalaxyEmrServiceException
from emr.service import EMRSchedulerService
from emr.service.constants import API_ROOT_PATH, DEFAULT_SERVICE_ENDPOINT
from emrthttpclient import EMRTHttpClient
from rpc.common.ttypes import ThriftProtocol, Version

'''
 Copyright 2015, Xiaomi.
 All rights reserved.
 Author: liupengcheng@xiaomi.com
'''
class ClientFactory:
  def __init__(self, credential=None, retry_if_timeout=False,
      thrift_protocol=ThriftProtocol.TBINARY):
    self.credential = credential
    self.retry_if_timeout = retry_if_timeout
    self.thrift_protocol = thrift_protocol
    ver = Version()
    version = "%s.%s,%s" % (ver.major, ver.minor, ver.patch)
    script = os.path.basename(sys.argv[0])
    if script:
      self.agent = "Python SDK/%s Python/%s(%s)" % (
        version, platform.python_version(), script
      )
    else:
      self.agent = "Python SDK/%s Python/%s" % (
      version, platform.python_version())

  def new_emr_service_client(self, url=DEFAULT_SERVICE_ENDPOINT + API_ROOT_PATH, socket_timeout=DEFAULT_CLIENT_TIMEOUT,
    max_retry=MAX_RETRY, support_account_key=False):
    return self.create_client(EMRSchedulerService.Iface, EMRSchedulerService.Client,
      url, socket_timeout, max_retry, support_account_key)


  def create_client(self, iface_class, client_class, url, socket_timeout,
      max_retry, support_account_key=False):
    thread_safe_client = ThreadSafeClient(client_class, self.credential, url, socket_timeout,
      self.agent, self.thrift_protocol, support_account_key)
    return RetryableClient(thread_safe_client, self.retry_if_timeout, max_retry)


class RetryableClient:
  def __init__(self, client, retryIfTimeout, max_retry):
    self.client = client
    self.retryIfTimeout = retryIfTimeout
    self.max_retry = max_retry

  def __getattr__(self, item):
    def __call_with_retries(*args):
      retry = 0
      while retry < self.max_retry:
        try:
          return getattr(self.client, item)(*args)
        except GalaxyEmrServiceException, ex:
          if ERROR_BACKOFF.get("timetosleep") and retry < self.max_retry-1:
            timetosleep = ERROR_BACKOFF["timetosleep"] / 1000 * (1 << retry)
            time.sleep(timetosleep)
            retry += 1
          else:
            raise ex
        except socket.timeout, se:
          if self.retryIfTimeout and retry < self.max_retry-1:
            retry += 1
          else:
            raise se

    return __call_with_retries


class ThreadSafeClient:
  def __init__(self, clazz, credential, url, timeout, agent, thrift_protocol, support_account_key):
    self.clazz = clazz
    self.credential = credential
    self.url = url
    self.timeout = timeout
    self.agent = agent
    self.protocol = thrift_protocol
    self.support_account_key = support_account_key

  def __getattr__(self, item):
    def __call_with_new_client(*args):
      requestId = self.generateRandomId(8)
      uri = '%s?id=%s&type=%s' % (self.url, requestId, item)
      http_client = EMRTHttpClient(self.credential, uri, self.timeout,
        self.protocol, self.support_account_key)
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
    return requestId[0:8]

