# encoding: utf-8
import os
import sys
import platform
import time
import socket
import random
import uuid

from sds.auth import AuthService
from sds.admin import AdminService
from sds.metrics.Common import EXCUTION_TIME
from sds.metrics.metricscollector import MetricsCollector
from sds.metrics.requestmetrics import RequestMetrics
from sds.table import TableService
from sds.table.ttypes import BatchOp
from sds.client.sdsthttpclient import SdsTHttpClient
from sds.common.constants import DEFAULT_SERVICE_ENDPOINT
from sds.common.constants import DEFAULT_ADMIN_CLIENT_TIMEOUT
from sds.common.constants import DEFAULT_CLIENT_TIMEOUT
from sds.common.constants import AUTH_SERVICE_PATH
from sds.common.constants import ADMIN_SERVICE_PATH
from sds.common.constants import TABLE_SERVICE_PATH
from sds.common.constants import Version
from sds.errors.constants import ERROR_BACKOFF
from sds.errors.constants import MAX_RETRY
from sds.client.exceptions import SdsTransportException
from sds.common.ttypes import ThriftProtocol
from sds.common.constants import THRIFT_PROTOCOL_MAP


class ClientFactory:
  def __init__(self, credential, retry_if_timeout=False, thrift_protocol=ThriftProtocol.TBINARY,
               is_metrics_enabled=False):
    self._credential = credential
    self._retry_if_timeout = retry_if_timeout
    self._protocol = thrift_protocol
    ver = Version()
    version = "%s.%s.%s" % (ver.major, ver.minor, ver.patch)
    script = os.path.basename(sys.argv[0])
    if script:
      self._agent = "Python-SDK/%s Python/%s(%s)" % \
                    (version, platform.python_version(), script)
    else:
      self._agent = "Python-SDK/%s Python/%s" % (version, platform.python_version())
    self._is_metrics_enabled = is_metrics_enabled
    self._metrics_collector = None

  def new_default_auth_client(self):
    return self.new_auth_client(DEFAULT_SERVICE_ENDPOINT + AUTH_SERVICE_PATH,
                                DEFAULT_CLIENT_TIMEOUT)

  def new_auth_client(self, url, timeout):
    client = self.get_client(AuthService.Client, url, timeout)
    return RetryableClient(client, self._retry_if_timeout)

  def new_default_admin_client(self):
    return self.new_admin_client(DEFAULT_SERVICE_ENDPOINT + ADMIN_SERVICE_PATH,
                                 DEFAULT_ADMIN_CLIENT_TIMEOUT)

  def new_admin_client(self, url, timeout):
    client = self.get_client(AdminService.Client, url, timeout)
    return RetryableClient(client, self._retry_if_timeout)

  def new_default_table_client(self):
    return self.new_table_client(DEFAULT_SERVICE_ENDPOINT + TABLE_SERVICE_PATH,
                                 DEFAULT_CLIENT_TIMEOUT)

  def new_table_client(self, url, timeout):
    client = self.get_client(TableService.Client, url, timeout)
    return RetryableClient(client, self._retry_if_timeout)

  def get_client(self, clazz, url, timeout):
    if self._is_metrics_enabled and self._metrics_collector is None:
      admin_client = ThreadSafeClient(AdminService.Client, self._credential, url, timeout, self._agent, self._protocol, None)
      metric_admin_client = RetryableClient(admin_client, True)
      self._metrics_collector = MetricsCollector(metric_admin_client)
    return ThreadSafeClient(clazz, self._credential, url, timeout, self._agent, self._protocol, self._metrics_collector)


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
        except SdsTransportException, ex:
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
  def __init__(self, clazz, credential, url, timeout, agent, thrift_protocol, metrics_collector):
    self.clazz = clazz
    self.credential = credential
    self.url = url
    self.timeout = timeout
    self.agent = agent
    self.protocol = thrift_protocol
    self.metrics_collector = metrics_collector

  def __getattr__(self, item):
    global request_metrics

    def __call_with_new_client(*args):
      query = self.getQuery(item, *args)
      uri = '%s?%s' % (self.url, query)
      http_client = SdsTHttpClient(self.credential, uri, self.timeout, self.protocol)
      http_client.setCustomHeaders({'User-Agent': self.agent})

      protocol_class = THRIFT_PROTOCOL_MAP[self.protocol]
      protocol_module = 'thrift.protocol.' + protocol_class
      mod = __import__(protocol_module, fromlist=[protocol_class])
      protocol_instance = getattr(mod, protocol_class)(http_client)
      client = self.clazz(protocol_instance, protocol_instance)
      return getattr(client, item)(*args)
    try:
      if self.metrics_collector is not None:
        request_metrics = RequestMetrics()
        request_metrics.start_event(EXCUTION_TIME)
        request_metrics.query_string = item
      return __call_with_new_client
    finally:
      if self.metrics_collector is not None:
        request_metrics.end_event(EXCUTION_TIME)
        self.metrics_collector.collect(request_metrics)

  def generateRandomId(self, length):
    requestId = str(uuid.uuid4())
    return requestId[0:8]

  def getQuery(self, item, *args):
    requestId = self.generateRandomId(8)
    requestType = item
    if item == 'get' or item == 'put' or item == 'increment' or item == 'scan' or item == 'remove' or item == 'putToRebuildIndex':
      return 'id=%s&type=%s&name=%s' % (requestId, requestType, args[0].tableName)
    elif item == 'batch':
      batchItems = args[0].items
      action = batchItems[0].action
      request = batchItems[0].request
      if action == BatchOp.PUT:
        tableName = request.putRequest.tableName
      elif action == BatchOp.GET:
        tableName = request.getRequest.tableName
      elif action == BatchOp.REMOVE:
        tableName = request.removeRequest.tableName
      elif action == BatchOp.INCREMENT:
        tableName = request.incrementRequest.tableName
      else:
        raise Exception("Unknown batch action: %s" % action)
      return 'id=%s&type=%s&name=%s' % (requestId, requestType, tableName)
    else:
      return 'id=%s&type=%s' % (requestId, requestType)