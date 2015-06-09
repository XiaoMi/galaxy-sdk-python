# encoding: utf-8
import base64
from sds.table.ttypes import DataType
from sds.table.ttypes import Datum
from sds.table.ttypes import Value
from sds.common.ttypes import ThriftProtocol


def datum(datavalue, datatype=None, thrift_protocol=ThriftProtocol.TBINARY):
  if datavalue is None:
    raise Exception("Datum must not be null")
  if datatype is None:
    if isinstance(datavalue, bool):
      return Datum(value=Value(boolValue=datavalue), type=DataType.BOOL)
    elif isinstance(datavalue, int):
      return Datum(value=Value(int32Value=datavalue), type=DataType.INT32)
    elif isinstance(datavalue, long):
      return Datum(value=Value(int64Value=datavalue), type=DataType.INT64)
    elif isinstance(datavalue, float):
      return Datum(value=Value(doubleValue=datavalue), type=DataType.DOUBLE)
    elif isinstance(datavalue, str):
      return Datum(value=Value(stringValue=datavalue), type=DataType.STRING)
    else:
      raise Exception("Unsupported data type: %s" % type(datavalue))
  else:
    if datatype == DataType.BOOL:
      return Datum(value=Value(boolValue=datavalue), type=datatype)
    elif datatype == DataType.INT8:
      return Datum(value=Value(int8Value=datavalue), type=datatype)
    elif datatype == DataType.INT16:
      return Datum(value=Value(int16Value=datavalue), type=datatype)
    elif datatype == DataType.INT32:
      return Datum(value=Value(int32Value=datavalue), type=datatype)
    elif datatype == DataType.INT64:
      return Datum(value=Value(int64Value=datavalue), type=datatype)
    elif datatype == DataType.FLOAT:
      return Datum(value=Value(doubleValue=datavalue), type=datatype)
    elif datatype == DataType.DOUBLE:
      return Datum(value=Value(doubleValue=datavalue), type=datatype)
    elif datatype == DataType.STRING:
      return Datum(value=Value(stringValue=datavalue), type=datatype)
    elif datatype == DataType.BINARY:
      # TODO support StringIO for complete binary support
      if thrift_protocol == ThriftProtocol.TBINARY \
          or thrift_protocol == ThriftProtocol.TCOMPACT:
        return Datum(value=Value(binaryValue=datavalue), type=datatype)
      else:
        return Datum(value=Value(binaryValue=base64.encodestring(datavalue)), type=datatype)
    elif datatype == DataType.BOOL_SET:
      return Datum(value=Value(boolSetValue=datavalue), type=datatype)
    elif datatype == DataType.INT8_SET:
      return Datum(value=Value(int8SetValue=datavalue), type=datatype)
    elif datatype == DataType.INT16_SET:
      return Datum(value=Value(int16SetValue=datavalue), type=datatype)
    elif datatype == DataType.INT32_SET:
      return Datum(value=Value(int32SetValue=datavalue), type=datatype)
    elif datatype == DataType.INT64_SET:
      return Datum(value=Value(int64SetValue=datavalue), type=datatype)
    elif datatype == DataType.FLOAT_SET:
      return Datum(value=Value(doubleSetValue=datavalue), type=datatype)
    elif datatype == DataType.DOUBLE_SET:
      return Datum(value=Value(doubleSetValue=datavalue), type=datatype)
    elif datatype == DataType.STRING_SET:
      return Datum(value=Value(stringSetValue=datavalue), type=datatype)
    else:
      raise Exception("Unsupported data type: %s" % datatype)


def value(dat, thrift_protocol=ThriftProtocol.TBINARY):
  if dat is None:
    return None

  datatype = dat.type
  datavalue = dat.value
  if datatype == DataType.BOOL:
    return datavalue.boolValue
  elif datatype == DataType.INT8:
    return datavalue.int8Value
  elif datatype == DataType.INT16:
    return datavalue.int16Value
  elif datatype == DataType.INT32:
    return datavalue.int32Value
  elif datatype == DataType.INT64:
    return datavalue.int64Value
  elif datatype == DataType.FLOAT:
    return datavalue.doubleValue
  elif datatype == DataType.DOUBLE:
    return datavalue.doubleValue
  elif datatype == DataType.STRING:
    return datavalue.stringValue
  elif datatype == DataType.BINARY:
    if thrift_protocol == ThriftProtocol.TBINARY \
        or thrift_protocol == ThriftProtocol.TCOMPACT:
      return datavalue.binaryValue
    else:
      return base64.decodestring(datavalue.binaryValue)
  elif datatype == DataType.BOOL_SET:
    return datavalue.boolSetValue
  elif datatype == DataType.INT8_SET:
    return datavalue.int8SetValue
  elif datatype == DataType.INT16_SET:
    return datavalue.int16SetValue
  elif datatype == DataType.INT32_SET:
    return datavalue.int32SetValue
  elif datatype == DataType.INT64_SET:
    return datavalue.int64SetValue
  elif datatype == DataType.FLOAT_SET:
    return datavalue.doubleSetValue
  elif datatype == DataType.DOUBLE_SET:
    return datavalue.doubleSetValue
  elif datatype == DataType.STRING_SET:
    return datavalue.stringSetValue
  else:
    raise Exception("Unsupported data type: %s" % datatype)


def values(dat, thrift_protocol=ThriftProtocol.TBINARY):
  return dict((k, value(dat[k], thrift_protocol)) for k in dat.iterkeys())
