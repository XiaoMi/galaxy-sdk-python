# encoding: utf-8
#
# Autogenerated by Thrift Compiler (0.9.2)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py:new_style
#

from thrift.Thrift import TType, TMessageType, TException, TApplicationException

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None


class ErrorCode(object):
  """
  List of ErrorCode.
  """
  INVALID_ACTION = 1
  INVALID_ATTRIBUTE = 2
  QUEUE_DELETED_RECENTLY = 3
  QUEUE_EXIST = 4
  QUEUE_NAME_MISSING = 5
  QUEUE_NOT_EXIST = 6
  QUEUE_INUSE = 7
  QUEUE_URI_CONFLICT = 8
  INVALID_RECEIPT_HANDLE = 9
  MESSAGE_BODY_MISSING = 10
  RECEIPT_HANDLE_NOT_EXIST = 11
  INDEX_NOT_UNIQUE = 12
  PERMISSION_DENIED = 13
  BAD_REQUEST = 34
  INTERNAL_ERROR = 14
  PARTITION_NOT_EXIST = 15
  PARTITION_NOT_RUNNING = 16
  QUEUE_NOT_CACHED = 17
  PARTITION_NOT_SERVING = 18
  TTRANSPORT_ERROR = 19
  UNKNOWN = 30

  _VALUES_TO_NAMES = {
    1: "INVALID_ACTION",
    2: "INVALID_ATTRIBUTE",
    3: "QUEUE_DELETED_RECENTLY",
    4: "QUEUE_EXIST",
    5: "QUEUE_NAME_MISSING",
    6: "QUEUE_NOT_EXIST",
    7: "QUEUE_INUSE",
    8: "QUEUE_URI_CONFLICT",
    9: "INVALID_RECEIPT_HANDLE",
    10: "MESSAGE_BODY_MISSING",
    11: "RECEIPT_HANDLE_NOT_EXIST",
    12: "INDEX_NOT_UNIQUE",
    13: "PERMISSION_DENIED",
    34: "BAD_REQUEST",
    14: "INTERNAL_ERROR",
    15: "PARTITION_NOT_EXIST",
    16: "PARTITION_NOT_RUNNING",
    17: "QUEUE_NOT_CACHED",
    18: "PARTITION_NOT_SERVING",
    19: "TTRANSPORT_ERROR",
    30: "UNKNOWN",
  }

  _NAMES_TO_VALUES = {
    "INVALID_ACTION": 1,
    "INVALID_ATTRIBUTE": 2,
    "QUEUE_DELETED_RECENTLY": 3,
    "QUEUE_EXIST": 4,
    "QUEUE_NAME_MISSING": 5,
    "QUEUE_NOT_EXIST": 6,
    "QUEUE_INUSE": 7,
    "QUEUE_URI_CONFLICT": 8,
    "INVALID_RECEIPT_HANDLE": 9,
    "MESSAGE_BODY_MISSING": 10,
    "RECEIPT_HANDLE_NOT_EXIST": 11,
    "INDEX_NOT_UNIQUE": 12,
    "PERMISSION_DENIED": 13,
    "BAD_REQUEST": 34,
    "INTERNAL_ERROR": 14,
    "PARTITION_NOT_EXIST": 15,
    "PARTITION_NOT_RUNNING": 16,
    "QUEUE_NOT_CACHED": 17,
    "PARTITION_NOT_SERVING": 18,
    "TTRANSPORT_ERROR": 19,
    "UNKNOWN": 30,
  }

class RetryType(object):
  SAFE = 0
  UNSAFE = 1
  UNSURE = 2

  _VALUES_TO_NAMES = {
    0: "SAFE",
    1: "UNSAFE",
    2: "UNSURE",
  }

  _NAMES_TO_VALUES = {
    "SAFE": 0,
    "UNSAFE": 1,
    "UNSURE": 2,
  }


class GalaxyEmqServiceException(TException):
  """
  Copyright 2015, Xiaomi.
  All rights reserved.
  Author: shenyuannan@xiaomi.com

  Attributes:
   - errorCode
   - errMsg
   - details
   - requestId
  """

  thrift_spec = (
    None, # 0
    (1, TType.I32, 'errorCode', None, None, ), # 1
    (2, TType.STRING, 'errMsg', None, None, ), # 2
    (3, TType.STRING, 'details', None, None, ), # 3
    (4, TType.STRING, 'requestId', None, None, ), # 4
  )

  def __init__(self, errorCode=None, errMsg=None, details=None, requestId=None,):
    self.errorCode = errorCode
    self.errMsg = errMsg
    self.details = details
    self.requestId = requestId

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I32:
          self.errorCode = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.errMsg = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRING:
          self.details = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.STRING:
          self.requestId = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('GalaxyEmqServiceException')
    if self.errorCode is not None:
      oprot.writeFieldBegin('errorCode', TType.I32, 1)
      oprot.writeI32(self.errorCode)
      oprot.writeFieldEnd()
    if self.errMsg is not None:
      oprot.writeFieldBegin('errMsg', TType.STRING, 2)
      oprot.writeString(self.errMsg)
      oprot.writeFieldEnd()
    if self.details is not None:
      oprot.writeFieldBegin('details', TType.STRING, 3)
      oprot.writeString(self.details)
      oprot.writeFieldEnd()
    if self.requestId is not None:
      oprot.writeFieldBegin('requestId', TType.STRING, 4)
      oprot.writeString(self.requestId)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __str__(self):
    return repr(self)

  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.errorCode)
    value = (value * 31) ^ hash(self.errMsg)
    value = (value * 31) ^ hash(self.details)
    value = (value * 31) ^ hash(self.requestId)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class Version(object):
  """
  Attributes:
   - major: The major version number;

   - minor: The minor version number;

   - revision: The revision number;

   - date: The date for release this version;

   - details: The version details;

  """

  thrift_spec = (
    None, # 0
    (1, TType.I32, 'major', None, 1, ), # 1
    (2, TType.I32, 'minor', None, 0, ), # 2
    (3, TType.I32, 'revision', None, 0, ), # 3
    (4, TType.STRING, 'date', None, "19700101", ), # 4
    (5, TType.STRING, 'details', None, "", ), # 5
  )

  def __init__(self, major=thrift_spec[1][4], minor=thrift_spec[2][4], revision=thrift_spec[3][4], date=thrift_spec[4][4], details=thrift_spec[5][4],):
    self.major = major
    self.minor = minor
    self.revision = revision
    self.date = date
    self.details = details

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I32:
          self.major = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I32:
          self.minor = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I32:
          self.revision = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.STRING:
          self.date = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 5:
        if ftype == TType.STRING:
          self.details = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('Version')
    if self.major is not None:
      oprot.writeFieldBegin('major', TType.I32, 1)
      oprot.writeI32(self.major)
      oprot.writeFieldEnd()
    if self.minor is not None:
      oprot.writeFieldBegin('minor', TType.I32, 2)
      oprot.writeI32(self.minor)
      oprot.writeFieldEnd()
    if self.revision is not None:
      oprot.writeFieldBegin('revision', TType.I32, 3)
      oprot.writeI32(self.revision)
      oprot.writeFieldEnd()
    if self.date is not None:
      oprot.writeFieldBegin('date', TType.STRING, 4)
      oprot.writeString(self.date)
      oprot.writeFieldEnd()
    if self.details is not None:
      oprot.writeFieldBegin('details', TType.STRING, 5)
      oprot.writeString(self.details)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.major is None:
      raise TProtocol.TProtocolException(message='Required field major is unset!')
    if self.minor is None:
      raise TProtocol.TProtocolException(message='Required field minor is unset!')
    if self.revision is None:
      raise TProtocol.TProtocolException(message='Required field revision is unset!')
    if self.date is None:
      raise TProtocol.TProtocolException(message='Required field date is unset!')
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.major)
    value = (value * 31) ^ hash(self.minor)
    value = (value * 31) ^ hash(self.revision)
    value = (value * 31) ^ hash(self.date)
    value = (value * 31) ^ hash(self.details)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)