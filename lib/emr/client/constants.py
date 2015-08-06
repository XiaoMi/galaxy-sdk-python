# !/usr/bin/env python
# encoding=utf-8

from collections import defaultdict

THRIFT_PROTOCOL_MAP = {
  0 : "TCompactProtocol",
  1 : "TJSONProtocol",
  2 : "TBinaryProtocol",
}

DEFAULT_CLIENT_TIMEOUT = 10000

MAX_RETRY = 3
ERROR_BACKOFF = defaultdict(lambda :1000)

SIGNATURE_MAP = {

}
# http headers
AUTHORIZATION = "authorization"
CONTENT_MD5 = "content-md5"
CONTENT_TYPE = "content-type"
CONTENT_LENGTH = "content-length"
MI_DATE = "x-xiaomi-date"
EXPIRES = "Expires"
RANGE = 'range'
USER_AGENT = "user-agent"
HOST = "host"
TIMESTAMP = "x-xiaomi-timestamp"

class SubResource(object):
  '''
  The sub-resource class.
  '''
  ACL = "acl"
  QUOTA = "quota"
  UPLOADS = "uploads"
  PART_NUMBER = "partNumber"
  UPLOAD_ID = "uploadId"
  STORAGE_ACCESS_TOKEN = "storageAccessToken"
  METADATA = "metadata"

  @staticmethod
  def get_all_subresource():
    return [SubResource.ACL,
      SubResource.QUOTA,
      SubResource.UPLOADS,
      SubResource.PART_NUMBER,
      SubResource.UPLOAD_ID,
      SubResource.STORAGE_ACCESS_TOKEN,
      SubResource.METADATA
    ]