# encoding: utf-8
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import httplib
import os
import socket
import sys
import urllib
import urlparse
import time
import hashlib
import base64
import hmac
from email.utils import formatdate
from collections import defaultdict
from cStringIO import StringIO

from thrift.transport.TTransport import TTransportBase
from thrift.transport.TTransport import TMemoryBuffer
from thrift.protocol.TJSONProtocol import TJSONProtocol
from constants import TIMESTAMP
from constants import MI_DATE
from constants import HOST
from constants import CONTENT_MD5
from constants import CONTENT_TYPE
from constants import CONTENT_LENGTH
from constants import AUTHORIZATION
from constants import SubResource
from rpc.auth.constants import HttpAuthorizationHeader
from rpc.auth.constants import SIGNATURE_SUPPORT
from rpc.auth.constants import MacAlgorithm
from rpc.common.constants import THRIFT_HEADER_MAP
from rpc.errors.ttypes import HttpStatusCode
from emr.exceptions.GalaxyEmrServiceException import GalaxyEmrServiceException


class EMRTHttpClient(TTransportBase):
  """Http implementation of TTransport base for EMR."""

  def __init__(self, credential, uri_or_host, timeout,
      thrift_protocol, support_account_key):
    self.credential = credential
    self.uri = uri_or_host
    parsed = urlparse.urlparse(uri_or_host)
    self.scheme = parsed.scheme
    assert self.scheme in ('http', 'https')
    if self.scheme == 'http':
      self.port = parsed.port or httplib.HTTP_PORT
    elif self.scheme == 'https':
      self.port = parsed.port or httplib.HTTPS_PORT
    self.host = parsed.hostname
    self.path = parsed.path
    if parsed.query:
      self.query = parsed.query
      self.path += '?%s' % parsed.query
    self.__timeout = timeout
    self.__protocol = thrift_protocol
    self.support_account_key = support_account_key
    self.__wbuf = StringIO()
    self.__http = None
    self.__custom_headers = None
    self.__clock_offset = 0

  def open(self):
    if self.scheme == 'http':
      self.__http = httplib.HTTP(self.host, self.port)
    else:
      self.__http = httplib.HTTPS(self.host, self.port)

  def close(self):
    self.__http.close()
    self.__http = None

  def isOpen(self):
    return self.__http is not None

  def setTimeout(self, ms):
    if not hasattr(socket, 'getdefaulttimeout'):
      raise NotImplementedError

    if ms is None:
      self.__timeout = None
    else:
      self.__timeout = ms / 1000.0

  def setCustomHeaders(self, headers):
    self.__custom_headers = headers

  def read(self, sz):
    return self.__http.file.read(sz)

  def write(self, buf):
    self.__wbuf.write(buf)

  def __withTimeout(f):
    def _f(*args, **kwargs):
      orig_timeout = socket.getdefaulttimeout()
      socket.setdefaulttimeout(args[0].__timeout)
      result = f(*args, **kwargs)
      socket.setdefaulttimeout(orig_timeout)
      return result

    return _f

  def flush(self):
    if self.isOpen():
      self.close()
    self.open()

    # Pull data out of buffer
    data = self.__wbuf.getvalue()
    self.__wbuf = StringIO()

    # HTTP request
    self.__http.putrequest('POST', self.path)

    headers = dict()
    # Write headers
    headers[HOST] = self.host
    headers[CONTENT_TYPE] = THRIFT_HEADER_MAP[self.__protocol]
    headers[CONTENT_LENGTH] = str(len(data))
    if not self.__custom_headers or 'User-Agent' not in self.__custom_headers:
      user_agent = 'Python/EMRTHttpClient'
      script = os.path.basename(sys.argv[0])
      if script:
        user_agent = '%s (%s)' % (user_agent, urllib.quote(script))
      headers['User-Agent'] = user_agent

    if self.__custom_headers:
      for key, val in self.__custom_headers.iteritems():
        headers[key] = val

    for k, v in headers.iteritems():
      self.__http.putheader(k, v)

    for key, val in self.__auth_headers(headers, data,
        self.support_account_key).iteritems():
      self.__http.putheader(key, val)

    self.__http.endheaders()

    # Write payload
    self.__http.send(data)

    # Get reply to flush the request
    code, message, headers = self.__http.getreply()
    if code != 200:
      if code == HttpStatusCode.CLOCK_TOO_SKEWED:
        server_time = float(headers[TIMESTAMP])
        local_time = time.time()
        self.__clock_offset = server_time - local_time
      raise GalaxyEmrServiceException(code, message)

  # Decorate if we know how to timeout
  if hasattr(socket, 'getdefaulttimeout'):
    flush = __withTimeout(flush)

  def __auth_headers(self, headers, body, support_account_key):
    auth_headers = dict()
    if self.credential and self.credential.type and self.credential.secretKeyId:
      if self.credential.type in SIGNATURE_SUPPORT:
        auth_headers[HOST] = self.host
        # timestamp
        auth_headers[TIMESTAMP] = str(int(time.time() + self.__clock_offset))
        auth_headers[MI_DATE] = formatdate(usegmt=True)
        # content md5
        auth_headers[CONTENT_MD5] = hashlib.md5(body).hexdigest()

        headers_to_sign = defaultdict(lambda :[])
        for k, v in headers.iteritems():
          headers_to_sign[str(k).lower()].append(v)

        for k, v in auth_headers.iteritems():
          headers_to_sign[str(k).lower()].append(v)

        signature = base64.b64encode(self.sign(self.__form_sign_content("POST", self.uri,
          headers_to_sign))).strip()
        auth_string = "Galaxy-V2 %s:%s" % (self.credential.secretKeyId, signature)

        auth_headers[AUTHORIZATION] = auth_string
      else:
        auth_header = HttpAuthorizationHeader()
        auth_header.secretKeyId = self.credential.secretKeyId
        auth_header.userType = self.credential.type
        auth_header.secretKey = self.credential.secretKey
        auth_header.supportAccountKey = support_account_key
        mb = TMemoryBuffer()
        protocol = TJSONProtocol(mb)
        auth_header.write(protocol)
        auth_headers[AUTHORIZATION] = str(mb.getvalue())
    return auth_headers

  def sign(self, content, algorithm=hashlib.sha1):
    return hmac.new(self.credential.secretKey, content, digestmod=algorithm).digest()


  def __form_sign_content(self, httpmethod, uri, headers):
    str_ = str()
    str_ += httpmethod + "\n"
    assert len(headers[CONTENT_MD5]) != 0, "content-md5 is required"
    str_ += headers[CONTENT_MD5][0] + "\n"
    assert len(headers[CONTENT_TYPE]) != 0, "content-type is required"
    str_ += headers[CONTENT_TYPE][0] + "\n"
    str_ += "\n"
    str_ += self.__canonical_xiaomi_headers(headers)
    str_ += self.__canonical_resource(uri)
    return str_

  def __canonical_xiaomi_headers(self, headers):
    if not headers:
      return ""

    single_mapping_headers = dict()
    for header, values in headers.iteritems():
      str_ = str()
      if not str(header).lower().startswith("x-xiaomi"):
        continue
      if type(values) is list:
        for val in values:
          str_ += str(val) if not str_ else "," + str(val)
      else:
        str_ = str(val)
      single_mapping_headers[header] = str_
    result = ""
    for k in sorted(single_mapping_headers):
      result += k + ":" + single_mapping_headers.get(k) + "\n"
    return result

  def __canonical_resource(self, uri):
    parsed = urlparse.urlparse(uri)
    result = parsed.path
    if not parsed.query or not parsed.query:
      return result
    params = dict()
    paramkvs = parsed.query.split('&')
    for paramkv in paramkvs:
      k, v = paramkv.split('=')
      params[k] = v
    is_first = True
    for k in sorted(params):
      if k in SubResource.get_all_subresource():
        if is_first:
          result += k
          is_first = False
        else:
          result += "&" + k
        result += "=" + params.get(k) if params.get(k) else ""

    return result
