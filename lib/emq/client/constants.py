XIAOMI_HEADER_PREFIX = "x-xiaomi-"
MI_DATE = "x-xiaomi-date"

GALAXY_ACCESS_KEY_ID = "GalaxyAccessKeyId"
SIGNATURE = "Signature"
EXPIRES = "Expires"

AUTHORIZATION = "authorization"
CONTENT_MD5 = "content-md5"
CONTENT_TYPE = "content-type"
DATE = "date"
RANGE = 'range'
USER_AGENT = "user-agent"
HOST = "host"
TIMESTAMP = "x-xiaomi-timestamp"

CACHE_CONTROL = "cache-control"
CONTENT_ENCODING = "content-encoding"
CONTENT_LENGTH = "content-length"
LAST_MODIFIED = "last-modified"

DEFAULT_CLIENT_TIMEOUT = 60000
DEFAULT_CLIENT_CONN_TIMEOUT = 30000

QUEUE_SERVICE_PATH = "/v1/api/queue"
MESSAGE_SERVICE_PATH = "/v1/api/message"
STATISTICS_SERVICE_PATH = "/v1/api/statistics"

DEFAULT_SERVICE_ENDPOINT = "http://emq.api.xiaomi.com"
DEFAULT_SECURE_SERVICE_ENDPOINT = "https://emq.api.xiaomi.com"

THRIFT_PROTOCOL_MAP = {
    0 : "TCompactProtocol",
    1 : "TJSONProtocol",
    2 : "TBinaryProtocol",
}

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