# encoding: utf-8
import time
from sds.admin.ttypes import ClientMetrics, MetricData, ClientMetricType
from sds.metrics.Common import EXCUTION_TIME


class RequestMetrics:
  def __init__(self):
    self._query_string = ''
    self.metrics = {}

  @property
  def query_string(self):
    return self._query_string

  @query_string.setter
  def query_string(self, value):
    self._query_string = value

  def start_event(self, metric_name):
    time_info = TimeInfo(self.milli_time(), None)
    self.metrics[metric_name] = time_info

  def end_event(self, metric_name):
    time_info = self.metrics.get(metric_name)
    time_info.end_time_milli = self.milli_time()

  def milli_time(self):
    return time.time() * 1000

  def to_client_metrics(self):
    client_metrics = ClientMetrics([])
    for k in self.metrics:
      metric_data = MetricData()
      if k == EXCUTION_TIME:
        metric_data.metricName = "%s.%s" % (self.query_string, k)
        metric_data.clientMetricType = ClientMetricType.Letency
        metric_data.value = (self.metrics[k].end_time_milli - self.metrics[k].start_time_milli) * 1000
        metric_data.timeStamp = round(self.metrics[k].end_time_milli / 1000)
      client_metrics.metricDataList.append(metric_data)
    return client_metrics

class TimeInfo:
  def __init__(self, start_time_milli, end_time_milli):
    self._start_time_milli = start_time_milli
    self._end_time_milli = end_time_milli

  @property
  def start_time_milli(self):
    return self._start_time_milli

  @property
  def end_time_milli(self):
    return self._end_time_milli

  @start_time_milli.setter
  def start_time_milli(self, time_milli):
    self._start_time_milli = time_milli

  @end_time_milli.setter
  def end_time_milli(self, time_milli):
    self._end_time_milli = time_milli