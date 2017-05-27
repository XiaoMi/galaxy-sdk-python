# encoding: utf-8
#
import base64
import http.client
import os
import socket
import sys
import time
import hashlib
import hmac
from io import BytesIO as StringIO
import urllib.request, urllib.parse, urllib.error
from emq.client.constants import CONTENT_LENGTH, CONTENT_TYPE, USER_AGENT, HOST, TIMESTAMP, CONTENT_MD5, MI_DATE, \
  XIAOMI_HEADER_PREFIX, AUTHORIZATION, SubResource
from emq.common.ttypes import GalaxyEmqServiceException
from rpc.common.constants import THRIFT_HEADER_MAP
from rpc.common.ttypes import ThriftProtocol
from rpc.errors.ttypes import HttpStatusCode
from urllib.parse import urlparse
from hashlib import sha1

from thrift.transport.TTransport import TTransportBase

def formatdate(timeval=None):
	"""Returns time format preferred for Internet standards.
		Sun, 06 Nov 1994 08:49:37 GMT  ; RFC 822, updated by RFC 1123
		According to RFC 1123, day and month names must always be in
		English.  If not for that, this code could use strftime().  It
		can't because strftime() honors the locale and could generated
		non-English names.
		"""
	if timeval is None: 
		timeval = time.time()
	timeval = time.gmtime(timeval)
	return "%s, %02d %s %04d %02d:%02d:%02d GMT" % ( 
			("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")[timeval[6]],timeval[2],
			("Jan", "Feb", "Mar", "Apr", "May", "Jun","Jul", "Aug", "Sep", "Oct", "Nov", 
			 "Dec")[timeval[1]-1], timeval[0], timeval[3], timeval[4], timeval[5])




class THttpClient(TTransportBase):
  """Http implementation of TTransport base for EMQ."""

  def __init__(self, credential, uri_or_host, timeout=None, thrift_protocol=ThriftProtocol.TBINARY):
    self.credential = credential
    parsed = urlparse(uri_or_host)
    self.scheme = parsed.scheme
    assert self.scheme in ('http', 'https')
    if self.scheme == 'http':
      self.port = parsed.port or http.client.HTTP_PORT
    elif self.scheme == 'https':
      self.port = parsed.port or http.client.HTTPS_PORT
    self.host = parsed.hostname
    self.path = parsed.path
    if parsed.query:
      self.path += '?%s' % parsed.query
    self.__timeout = timeout
    self.__protocol = thrift_protocol
    self.__wbuf = StringIO()
    self.__http = None
    self.__custom_headers = None
    self.__response = None
    self.__clock_offset = 0

  def open(self):
    if self.scheme == 'http':
      self.__http = http.client.HTTPConnection(self.host, self.port)
    else:
      self.__http = http.client.HTTPSConnection(self.host, self.port)

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
    return self.__response.read(sz)

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

    default_user_agent = 'Python/THttpClient'
    if not self.__custom_headers or USER_AGENT not in self.__custom_headers:
      user_agent = default_user_agent
      script = os.path.basename(sys.argv[0])
      if script:
        user_agent = '%s (%s)' % (user_agent, urllib.parse.quote(script))
      self.__http.putheader(USER_AGENT, user_agent)
      for key, val in list(self.__auth_headers(dict(headers.items() | self.__custom_headers.items())).items()):
        self.__http.putheader(key, val)

    self.__http.endheaders()

    # Write payload
    self.__http.send(data)

    # Get reply to flush the request
    self.__response = self.__http.getresponse() 
#status = response.
#message = response.reason
    # print "http get reply is:%s" % self.__http.getreply

  # Decorate if we know how to timeout
  if hasattr(socket, 'getdefaulttimeout'):
    flush = __withTimeout(flush)

  def __auth_headers(self, headers):
    string_to_assign = str()
    string_to_assign += '%s\n' % 'POST'
    string_to_assign += '%s\n' % headers[CONTENT_MD5]
    string_to_assign += '%s\n' % headers[CONTENT_TYPE]
    string_to_assign += '\n'
    string_to_assign += '%s' % self.__canonicalize_xiaomi_headers(headers)
    string_to_assign += '%s' % self.__canonicalize_resource(self.path)
    signature = \
      base64.encodestring(hmac.new(bytearray(self.credential.secretKey.encode('utf-8')),bytes(string_to_assign.encode('utf-8')), digestmod=sha1).digest()).strip()
    auth_string = "Galaxy-V2 %s:%s" % (self.credential.secretKeyId, signature.decode('utf-8'))
    headers[AUTHORIZATION] = auth_string

    return headers

  def __set_headers(self, body):
    headers = dict()
    headers[HOST] = self.host
    headers[CONTENT_LENGTH] = str(len(body))
    headers[TIMESTAMP] = str(int(time.time() + self.__clock_offset))
    headers[CONTENT_MD5] = hashlib.md5(body).hexdigest()
    headers[CONTENT_TYPE] = THRIFT_HEADER_MAP[self.__protocol]
    headers[MI_DATE] = formatdate(time.time())
    return headers

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

    i = 0
    for query in sorted(query_args):
      key = query.split('=')
      if key[0] in SubResource.get_all_subresource():
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
