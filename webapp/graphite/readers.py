import os
import time
from graphite.intervals import Interval, IntervalSet
from graphite.carbonlink import CarbonLink
from graphite.logger import log
from django.conf import settings
from graphite.settings import HBASE_CONFIG
from multiprocessing import Pool

try:
  import whisper
except ImportError:
  whisper = False

try:
  import rrdtool
except ImportError:
  rrdtool = False

try:
  import gzip
except ImportError:
  gzip = False

try:
  import happybase
except ImportError:
  happybase = False

class FetchInProgress(object):
  def __init__(self, wait_callback):
    self.wait_callback = wait_callback

  def waitForResults(self):
    return self.wait_callback()


class MultiReader(object):
  __slots__ = ('nodes',)

  def __init__(self, nodes):
    self.nodes = nodes

  def get_intervals(self):
    interval_sets = []
    for node in self.nodes:
      interval_sets.extend( node.intervals.intervals )
    return IntervalSet( sorted(interval_sets) )

  def fetch(self, startTime, endTime):
    # Start the fetch on each node
    results = [ n.fetch(startTime, endTime) for n in self.nodes ]

    # Wait for any asynchronous operations to complete
    for i, result in enumerate(results):
      if isinstance(result, FetchInProgress):
        try:
          results[i] = result.waitForResults()
        except:
          log.exception("Failed to complete subfetch")
          results[i] = None

    results = [r for r in results if r is not None]
    if not results:
      raise Exception("All sub-fetches failed")

    return reduce(self.merge, results)

  def merge(self, results1, results2):
    # Ensure results1 is finer than results2
    if results1[0][2] > results2[0][2]:
      results1, results2 = results2, results1

    time_info1, values1 = results1
    time_info2, values2 = results2
    start1, end1, step1 = time_info1
    start2, end2, step2 = time_info2

    step   = step1                # finest step
    start  = min(start1, start2)  # earliest start
    end    = max(end1, end2)      # latest end
    time_info = (start, end, step)
    values = []

    t = start
    while t < end:
      # Look for the finer precision value first if available
      i1 = (t - start1) / step1

      if len(values1) > i1:
        v1 = values1[i1]
      else:
        v1 = None

      if v1 is None:
        i2 = (t - start2) / step2

        if len(values2) > i2:
          v2 = values2[i2]
        else:
          v2 = None

        values.append(v2)
      else:
        values.append(v1)

      t += step

    return (time_info, values)


class CeresReader(object):
  __slots__ = ('ceres_node', 'real_metric_path')
  supported = True

  def __init__(self, ceres_node, real_metric_path):
    self.ceres_node = ceres_node
    self.real_metric_path = real_metric_path

  def get_intervals(self):
    intervals = []
    for info in self.ceres_node.slice_info:
      (start, end, step) = info
      intervals.append( Interval(start, end) )

    return IntervalSet(intervals)

  def fetch(self, startTime, endTime):
    data = self.ceres_node.read(startTime, endTime)
    time_info = (data.startTime, data.endTime, data.timeStep)
    values = list(data.values)

    # Merge in data from carbon's cache
    try:
      cached_datapoints = CarbonLink.query(self.real_metric_path)
    except:
      log.exception("Failed CarbonLink query '%s'" % self.real_metric_path)
      cached_datapoints = []

    for (timestamp, value) in cached_datapoints:
      interval = timestamp - (timestamp % data.timeStep)

      try:
        i = int(interval - data.startTime) / data.timeStep
        values[i] = value
      except:
        pass

    return (time_info, values)


class WhisperReader(object):
  __slots__ = ('fs_path', 'real_metric_path')
  supported = bool(whisper)

  def __init__(self, fs_path, real_metric_path):
    self.fs_path = fs_path
    self.real_metric_path = real_metric_path

  def get_intervals(self):
    start = time.time() - whisper.info(self.fs_path)['maxRetention']
    end = max( os.stat(self.fs_path).st_mtime, start )
    return IntervalSet( [Interval(start, end)] )

  def fetch(self, startTime, endTime):
    data = whisper.fetch(self.fs_path, startTime, endTime)
    if not data:
      return None

    time_info, values = data
    (start,end,step) = time_info

    meta_info = whisper.info(self.fs_path)
    lowest_step = min([i['secondsPerPoint'] for i in meta_info['archives']])
    # Merge in data from carbon's cache
    cached_datapoints = []
    try:
        if step == lowest_step:
            cached_datapoints = CarbonLink.query(self.real_metric_path)
    except:
      log.exception("Failed CarbonLink query '%s'" % self.real_metric_path)
      cached_datapoints = []

    if isinstance(cached_datapoints, dict):
      cached_datapoints = cached_datapoints.items()

    for (timestamp, value) in cached_datapoints:
      interval = timestamp - (timestamp % step)

      try:
        i = int(interval - start) / step
        values[i] = value
      except:
        pass

    return (time_info, values)


class GzippedWhisperReader(WhisperReader):
  supported = bool(whisper and gzip)

  def get_intervals(self):
    fh = gzip.GzipFile(self.fs_path, 'rb')
    try:
      info = whisper.__readHeader(fh) # evil, but necessary.
    finally:
      fh.close()

    start = time.time() - info['maxRetention']
    end = max( os.stat(self.fs_path).st_mtime, start )
    return IntervalSet( [Interval(start, end)] )

  def fetch(self, startTime, endTime):
    fh = gzip.GzipFile(self.fs_path, 'rb')
    try:
      return whisper.file_fetch(fh, startTime, endTime)
    finally:
      fh.close()


class RRDReader:
  supported = bool(rrdtool)

  def __init__(self, fs_path, datasource_name):
    self.fs_path = fs_path
    self.datasource_name = datasource_name

  def get_intervals(self):
    start = time.time() - self.get_retention(self.fs_path)
    end = max( os.stat(self.fs_path).st_mtime, start )
    return IntervalSet( [Interval(start, end)] )

  def fetch(self, startTime, endTime):
    startString = time.strftime("%H:%M_%Y%m%d+%Ss", time.localtime(startTime))
    endString = time.strftime("%H:%M_%Y%m%d+%Ss", time.localtime(endTime))

    if settings.FLUSHRRDCACHED:
      rrdtool.flushcached(self.fs_path, '--daemon', settings.FLUSHRRDCACHED)

    (timeInfo, columns, rows) = rrdtool.fetch(self.fs_path,settings.RRD_CF,'-s' + startString,'-e' + endString)
    colIndex = list(columns).index(self.datasource_name)
    rows.pop() #chop off the latest value because RRD returns crazy last values sometimes
    values = (row[colIndex] for row in rows)

    return (timeInfo, values)

  @staticmethod
  def get_datasources(fs_path):
    info = rrdtool.info(fs_path)

    if 'ds' in info:
      return [datasource_name for datasource_name in info['ds']]
    else:
      ds_keys = [ key for key in info if key.startswith('ds[') ]
      datasources = set( key[3:].split(']')[0] for key in ds_keys )
      return list(datasources)

  @staticmethod
  def get_retention(fs_path):
    info = rrdtool.info(fs_path)
    if 'rra' in info:
      rras = info['rra']
    else:
      # Ugh, I like the old python-rrdtool api better..
      rra_count = max([ int(key[4]) for key in info if key.startswith('rra[') ]) + 1
      rras = [{}] * rra_count
      for i in range(rra_count):
        rras[i]['pdp_per_row'] = info['rra[%d].pdp_per_row' % i]
        rras[i]['rows'] = info['rra[%d].rows' % i]

    retention_points = 0
    for rra in rras:
      points = rra['pdp_per_row'] * rra['rows']
      if points > retention_points:
        retention_points = points

    return  retention_points * info['step']


def _avg(data):
  if isinstance(data, list) and len(data) > 0:
    return float(sum(data) / len(data))
  else:
    return float('nan')


def _last(data):
  if isinstance(data, list) and len(data) > 0:
    return float(data[-1])
  else:
    return float('nan')


def _scan_table(t):
  cur_step = t['r'][0]
  cur_start = t['start']
  cur_end = t['end']
  start_floor = int(cur_start / 7200) * 7200
  # row_stop in a scan is exclusive, so add an extra hour
  end_floor = int(cur_end / 7200) * 7200 + 7200
  length = int((cur_end - cur_start) / cur_step)
  data_val = [None] * length
  # Defalut to 'avg'
  method = t['method']
  if 'max' in method:
    method = max
  elif 'min' in method:
    method = min
  elif 'sum' in method:
    method = sum
  elif 'last' in method:
    method = _last
  else:
    method = _avg

  start_key = "%s:%d" % (t['metric'], start_floor)
  end_key = "%s:%d" % (t['metric'], end_floor)
  time_info = (cur_start, cur_end, cur_step)

  """
  Each row in the table has timestamps written at the smallest
  retention rate. So we "slot" the data into the step-"floored"
  timestamp and roll it up as we go.
  """
  client = happybase.Connection(host=HBASE_CONFIG['host'],
                                port=HBASE_CONFIG['port'],
                                table_prefix='graphite',
                                transport=HBASE_CONFIG['transport_type'],
                                compat=HBASE_CONFIG['compat_level'],
                                protocol=HBASE_CONFIG['protocol'])

  for row in client.table(t['name']).scan(row_start=start_key,
                                          row_stop=end_key):
    key, data = row
    for ts, val in data.iteritems():
      # data is serialized on the way back, so we'll need to format
      val = float(val)
      group_ts = int(ts.split(":")[1])
      slot = int(((group_ts - cur_start) / cur_step) % length)
      if data_val[slot]:
        points = [data_val[slot], val]
        data_val[slot] = method(points)
      else:
        data_val[slot] = val
  return time_info, data_val


class HBaseReader(object):
  __slots__ = ('metric', 'retentions', 'method')

  def __init__(self, metric, retentions, method):
    self.metric = metric
    self.retentions = retentions
    self.method = method

  def get_intervals(self):
    # We can only go back as far as the oldest retention
    ret = int(sum([r[0] * r[1] for r in self.retentions]))
    return IntervalSet([Interval(ret, time.time())])

  def fetch(self, startTime, endTime):
    default_table = {'metric': self.metric,
                     'method': self.method}
    table_config = self._table_config(default_table, startTime, endTime)

    threads = Pool(len(table_config))
    results = threads.map(_scan_table, table_config)
    threads.close()
    threads.join()

    # No reason to call a function if we don't have to
    if len(results) > 1:
        return reduce(self._merge, results)
    else:
        return results[0]

  def _merge(self, results1, results2):
    # Ensure results1 is finer than results2
    if results1[0][2] < results2[0][2]:
      results1, results2 = results2, results1

    time_info1, values1 = results1
    time_info2, values2 = results2
    start1, end1, step1 = time_info1
    start2, end2, step2 = time_info2
    # finest step
    step = int(min(step1, step2))
    # earliest start
    start = int(min(start1, start2))
    # latest end
    end = int(max(end1, end2))
    time_info = (start, end, step)
    values = []
    for t in xrange(start, end, step):
      # Look for the finer precision value first if available
      i1 = int((t - start1) / step1)
      if len(values1) > i1 > 0:
        v1 = values1[i1]
      else:
        v1 = None
      if v1 is None:
        i2 = int((t - start2) / step2)
        if len(values2) > i2 > 0:
          v2 = values2[i2]
        else:
          v2 = None
        values.append(v2)
      else:
        values.append(v1)
    return time_info, values

  def _table_config(self, default_table, startTime, endTime):
    now = time.time()
    table_config = []
    offset = 0
    reten_str = ".".join("%s_%s" % tup for tup in self.retentions)
    for r in self.retentions:
      r_secs = int(int(r[0]) * int(r[1]))
      cur_end = now - offset
      offset += r_secs
      cur_start = now - offset
      # The table is completely outside our window
      if cur_end < startTime or cur_start > endTime:
        continue

      # build the table config
      cur_table = dict(default_table)
      cur_table['name'] = "%d.%s" % (r[0], reten_str)
      cur_table['r'] = r
      if endTime < cur_end:
        cur_table['end'] = endTime
      else:
        cur_table['end'] = cur_end
      if cur_start < startTime:
        cur_table['start'] = startTime
      else:
        cur_table['start'] = cur_start
      table_config.append(cur_table)
    return table_config
