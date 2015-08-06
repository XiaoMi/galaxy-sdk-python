#!/usr/bin/env python
#encoding=utf-8

from rpc.errors.ttypes import TException
class GalaxyEmrServiceException(TException):
  def __init__(self, code, message):
    super(TException, self).__init__(message)
    self.code = code
    self.message = message

  def get_status_code(self):
    return self.code

  def get_message(self):
    return self.message

  def __repr__(self):
    return "GalaxyEmrServiceException:(code:%d, message:%s)" % (self.code, self.message)

  def __str__(self):
    return self.__repr__()

