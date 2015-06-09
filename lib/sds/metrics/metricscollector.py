import Queue
import time
from sds.admin.ttypes import ClientMetrics
import threading
from sds.metrics.Common import UPLOAD_INTERVAL


class MetricsCollector:
  def __init__(self, metric_admin_client):
    self.queue = Queue.Queue(0)
    self.metric_admin_client = metric_admin_client
    metric_upload_thread = MetricUploadThread(self.queue, self.metric_admin_client)
    metric_upload_thread.setDaemon(True)
    metric_upload_thread.start()

  def collect(self, request_metrics):
    client_metrics = request_metrics.to_client_metrics()
    for k in client_metrics.metricDataList:
      self.queue.put(k)


class MetricUploadThread(threading.Thread):
  def __init__(self, queue, metric_admin_client):
    super(MetricUploadThread, self).__init__()
    self.queue = queue
    self.name = "sds-python-sdk-metrics-uploader"
    self.metric_admin_client = metric_admin_client

  def run(self):
    while True:
      try:
        start_time = time.time() * 1000
        client_metrics = ClientMetrics()
        metrics_data_list = []
        while True:
          elapsed_time = time.time() * 1000 - start_time
          if elapsed_time > UPLOAD_INTERVAL:
            break
          else:
            try:
              metricData = self.queue.get(True, (UPLOAD_INTERVAL - elapsed_time) / 1000)
            except Queue.Empty as em:
              break
            metrics_data_list.append(metricData)
        client_metrics.metricDataList = metrics_data_list
        self.metric_admin_client.putClientMetrics(client_metrics)
      except Exception as e:
        pass