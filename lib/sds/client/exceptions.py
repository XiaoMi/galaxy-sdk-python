# encoding: utf-8
from sds.errors.constants import HttpStatusCode
from sds.errors.constants import ErrorCode


class SdsTransportException(Exception):
  def __init__(self):
    self.httpStatusCode = None
    self.errorCode = None
    self.errorMessage = None

  def __init__(self, http_status_code=None, error_message=None):
    self.httpStatusCode = http_status_code
    self.errorMessage = error_message
    if http_status_code == HttpStatusCode.INVALID_AUTH:
      self.errorCode = ErrorCode.INVALID_AUTH
    elif http_status_code == HttpStatusCode.CLOCK_TOO_SKEWED:
      self.errorCode = ErrorCode.CLOCK_TOO_SKEWED
    elif http_status_code == HttpStatusCode.REQUEST_TOO_LARGE:
      self.errorCode = ErrorCode.REQUEST_TOO_LARGE
    elif http_status_code == HttpStatusCode.INTERNAL_ERROR:
      self.errorCode = ErrorCode.INTERNAL_ERROR
    elif http_status_code == HttpStatusCode.BAD_REQUEST:
      self.errorCode = ErrorCode.BAD_REQUEST
    else:
      self.errorCode = ErrorCode.UNKNOWN

  def __str__(self):
    return repr(self)

  def __repr__(self):
    L = ['%s=%r' % (key, value)
         for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))
