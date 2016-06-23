import os
import time
from graphite.intervals import Interval, IntervalSet
from graphite.carbonlink import CarbonLink
from graphite.logger import log
from django.conf import settings

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
  if len(data) > 0:
    return float(sum(data) / len(data))
  else:
    return float('nan')


def _scan_table(t):
  thrift_cfg = t['thrift']
  cur_step = t['r'][0]
  final_step = t['step']
  start_floor = int((t['start'] / 7200) * 7200)
  # row_stop in a scan is exclusive, so add an extra hour
  end_floor = int((t['end'] / 7200) * 7200) + 7200
  data_val = []
  # Defalut to 'avg'
  if 'max' in t['method']:
    method = max
  elif 'min' in t['method']:
    method = min
  elif 'sum' in t['method']:
    method = sum
  else:
    method = _avg

  if final_step < cur_step:
    step = int(cur_step / final_step)
  else:
    step = int(final_step / cur_step)

  start_key = "%s:%d" % (t['metric'], start_floor)
  end_key = "%s:%d" % (t['metric'], end_floor)
  time_info = (t['start'], t['end'], cur_step)
  """
  Each row in the table has timestamps written at the smallest
  retention rate. So we'll need to take slices of each row at
  the difference between the current data rate and our "final"
  data rate and aggregate those appropriately.
  """
  client = happybase.Connection(
               host=thrift_cfg['thrift_host'],
               port=thrift_cfg['port'],
               table_prefix=thrift_cfg['table_prefix'],
               transport=thrift_cfg['transport'],
               protocol=thrift_cfg['protocol'])
  for row in client.table(t['name']).scan(row_start=start_key,
                                          row_stop=end_key):
    key, data = row
    data = [float(data[k]) for k in sorted(data.iterkeys())]
    rowlen = len(data)
    for pos in xrange(0, rowlen, step):
      if pos + step >= rowlen:
        group = data[pos:]
      else:
        group = data[pos:pos + step]
      data_val.append(method(group))
  return (time_info, data_val)


class HBaseReader():
  __slots__ = ('metric', 'thriftconfig', 'retentions', 'method')

  def __init__(self, metric, retentions, method, thrift_config):
    self.metric = metric
    self.thriftconfig = thrift_config
    self.retentions = retentions
    self.method = method

  def get_intervals(self):
    # We can only go back as far as the oldest retention
    ret = sum([r[0] * r[1] for r in self.retentions])
    return [ (int(ret), int(time())) ]

  def fetch(self, startTime, endTime, now):
    if startTime > endTime:
      log.exception("Invalid time interval: from time '%s' is after " +
                    "until time '%s'" % (startTime, endTime))
      return
    if startTime > now:  # from time in the future
      log.exception("Invalid time interval: from time '%s' is " +
                    "in the future!" % startTime)
      return
    if endTime is None or endTime > now:
      endTime = now

    default_table = {'metric': self.metric,
                     'thrift': self.thriftconfig,
                     'method': self.method}
    table_config = self._table_config(default_table, startTime,
                                      endTime, now)
    threads = Pool(len(table_config))
    results = threads.map(_scan_table, table_config)
    threads.close()
    threads.join()
    # No reason to call a function if we don't have to
    if len(results) > 1:
        return reduce(self._merge, results)
    else:
        return results

  def _merge(self, results1, results2):
    # Ensure results1 is finer than results2
    if results1[0][2] < results2[0][2]:
      results1, results2 = results2, results1

    time_info1, values1 = results1
    time_info2, values2 = results2
    start1, end1, step1 = time_info1
    start2, end2, step2 = time_info2
    step = step1                # finest step
    start = min(start1, start2)  # earliest start
    end = max(end1, end2)      # latest end
    time_info = (start, end, step)
    values = []

    for t in xrange(start, end, step):
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
    return time_info, values

  def _table_config(self, default_table, startTime, endTime, now):
    table_config = []
    offset = 0
    # Start with coarse retention
    final_step = self.retentions[-1][0]
    reten_str = ".".join("%s_%s" % tup for tup in self.retentions)
    for r in self.retentions:
      r_secs = int(int(r[0]) * int(r[1]))
      cur_end = now - offset
      offset += r_secs
      cur_start = now - offset
      # Everything beyond now will be outside our timeframe
      if cur_end < startTime:
        break
      # Basically, everything in this table is in the future
      if cur_start > endTime:
        continue
      cur_table = dict(default_table)
      cur_table['name'] = "%d.%s" % (r[0], reten_str)
      if r[0] < final_step:
        final_step = r[0]
      cur_table['r'] = r
      cur_table['end'] = cur_end
      if cur_start < startTime:
        cur_table['start'] = startTime
      else:
        cur_table['start'] = cur_start
      table_config.append(cur_table)
    # Add the step we'll shooting for to each table
    for l in table_config:
      l['step'] = final_step
    return table_config
