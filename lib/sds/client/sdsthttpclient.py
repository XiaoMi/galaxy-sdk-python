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
import base64
import httplib
import os
import rfc822
import socket
import sys
import urllib
import urlparse
import time
import hashlib
import hmac
from cStringIO import StringIO

from thrift.transport.TTransport import TTransportBase
from sds.auth.constants import HK_TIMESTAMP
from sds.auth.constants import HK_HOST
from sds.auth.constants import HK_CONTENT_MD5
from sds.auth.constants import HK_AUTHORIZATION
from sds.auth.constants import MI_DATE
from sds.auth.constants import XIAOMI_HEADER_PREFIX
from sds.errors.constants import HttpStatusCode
from sds.client.exceptions import SdsTransportException
from sds.common.ttypes import ThriftProtocol
from sds.common.constants import THRIFT_HEADER_MAP
from urlparse import urlparse
from hashlib import sha1


class SdsTHttpClient(TTransportBase):
  """Http implementation of TTransport base for SDS."""

  def __init__(self, credential, uri_or_host, timeout=None, thrift_protocol=ThriftProtocol.TBINARY):
    self.credential = credential
    parsed = urlparse(uri_or_host)
    self.scheme = parsed.scheme
    assert self.scheme in ('http', 'https')
    if self.scheme == 'http':
      self.port = parsed.port or httplib.HTTP_PORT
    elif self.scheme == 'https':
      self.port = parsed.port or httplib.HTTPS_PORT
    self.host = parsed.hostname
    self.path = parsed.path
    if parsed.query:
      self.path += '?%s' % parsed.query
    self.__timeout = timeout
    self.__protocol = thrift_protocol
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

    # Write headers
    headers = self.__set_headers(data)

    if not self.__custom_headers or 'User-Agent' not in self.__custom_headers:
      user_agent = 'Python/THttpClient'
      script = os.path.basename(sys.argv[0])
      if script:
        user_agent = '%s (%s)' % (user_agent, urllib.quote(script))
      self.__http.putheader('User-Agent', user_agent)

    for key, val in self.__auth_headers(dict(headers.items() + self.__custom_headers.items())).iteritems():
      self.__http.putheader(key, val)

    self.__http.endheaders()

    # Write payload
    self.__http.send(data)

    # Get reply to flush the request
    code, message, headers = self.__http.getreply()
    if code != 200:
      if code == HttpStatusCode.CLOCK_TOO_SKEWED:
        server_time = float(headers[HK_TIMESTAMP])
        local_time = time.time()
        self.__clock_offset = server_time - local_time
      raise SdsTransportException(code, message)

  # Decorate if we know how to timeout
  if hasattr(socket, 'getdefaulttimeout'):
    flush = __withTimeout(flush)

  def __auth_headers(self, headers):
    string_to_assign = str()
    string_to_assign += '%s\n' % 'POST'
    string_to_assign += '%s\n' % self.__get_header(headers, "content-md5")
    string_to_assign += '%s\n' % self.__get_header(headers, "content-type")
    string_to_assign += '\n'
    string_to_assign += '%s' % self.__canonicalize_xiaomi_headers(headers)
    string_to_assign += '%s' % self.__canonicalize_resource(self.path)
    signature = \
      base64.encodestring(hmac.new(self.credential.secretKey, string_to_assign, digestmod=sha1).digest()).strip()
    auth_string = "Galaxy-V3 %s:%s" % (self.credential.secretKeyId, signature)
    headers[HK_AUTHORIZATION] = auth_string

    return headers

  def __set_headers(self, body):
    headers = dict()
    headers[HK_HOST] = self.host
    headers['content-length'] = str(len(body))
    headers[HK_TIMESTAMP] = str(int(time.time() + self.__clock_offset))
    headers[HK_CONTENT_MD5] = hashlib.md5(body).hexdigest()
    headers['content-type'] = THRIFT_HEADER_MAP[self.__protocol]
    headers[MI_DATE] = rfc822.formatdate(time.time())
    return headers

  def __get_header(self, http_headers, header_name):
    if http_headers is None or len(http_headers) == 0:
      return ''
    for key in http_headers:
      lower_key = key.lower()
      try:
        lower_key = lower_key.decode('utf-8')
      except:
        pass
      if lower_key == header_name and http_headers[key]:
        if type(http_headers[key]) != str:
          return http_headers[key][0]
        else:
          return http_headers[key]
    return ''


  def __canonicalize_xiaomi_headers(self, http_headers):
    if http_headers is None or len(http_headers) == 0:
      return ''

    canonicalized_headers = dict()
    for key in http_headers:
      lower_key = key.lower()
      try:
        lower_key = lower_key.decode('utf-8')
      except:
        pass

      if http_headers[key] and lower_key.startswith(XIAOMI_HEADER_PREFIX):
        if type(http_headers[key]) != str:
          canonicalized_headers[lower_key] = str()
          i = 0
          for k in http_headers[key]:
            canonicalized_headers[lower_key] += '%s' % (k.strip())
            i += 1
            if i < len(http_headers[key]):
              canonicalized_headers[lower_key] += ','
        else:
          canonicalized_headers[lower_key] = http_headers[key].strip()

    result = ""
    for key in sorted(canonicalized_headers.keys()):
      values = canonicalized_headers[key]
      result += '%s:%s\n' % (key, values)
    return result

  def __canonicalize_resource(self, uri):
    result = ""
    parsed_url = urlparse(uri)
    result += '%s' % parsed_url.path
    query_args = parsed_url.query.split('&')
    subresource = ['acl', 'quota', 'uploads', 'partNumber', 'uploadId', 'storageAccessToken', 'metadata']

    i = 0
    for query in sorted(query_args):
      key = query.split('=')
      if key[0] in subresource:
        if i == 0:
          result += '?'
        else:
          result += '&'
        if len(key) == 1:
          result += '%s' % key[0]
        else:
          result += '%s=%s' % (key[0], key[1])
        i += 1
    return result
