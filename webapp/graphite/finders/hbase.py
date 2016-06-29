import happybase
import json
from . import match_entries
from graphite.node import BranchNode, LeafNode
from django.conf import settings
from graphite.logger import log
from graphite.readers import HBaseReader
from graphite.settings import HBASE_CONFIG

META_CF_NAME = "m"
COLUMN_NAME = "%s:NODE" % META_CF_NAME
RETEN_NAME = "%s:AGG" % META_CF_NAME
METHOD_NAME = "%s:AGG_METHOD" % META_CF_NAME

class HBaseFinder(object):
  __slots__ = ('store_table')

  def find_nodes(self, query):
    client = happybase.Connection(host=HBASE_CONFIG['host'],
                                  port=HBASE_CONFIG['port'],
                                  table_prefix='graphite',
                                  transport=HBASE_CONFIG['transport_type'],
                                  compat=HBASE_CONFIG['compat_level'],
                                  protocol=HBASE_CONFIG['protocol'])
    self.store_table = client.table('META')
    log.info("Made connection to HBase")
    # break query into parts
    pattern_parts = self._cheaper_patterns(query.pattern.split("."))
    if pattern_parts[0] in ["*", "ROOT"]:
      start_string = "ROOT"
    else:
      start_string = "%s" % pattern_parts[0]
    pattern_parts = pattern_parts[1:]
    """
    The actual gets occur in _find_paths, so this for loop
    should only be processing whether we return branch
    or leaf nodes to the calling function.
    """
    for part, subnodes in self._find_paths(start_string, pattern_parts):
      if COLUMN_NAME not in subnodes.keys():
        this_node = subnodes[part]
        row = self.store_table.row(this_node)
      else:
        this_node = part
        row = dict(subnodes)
      if not bool(row.get(COLUMN_NAME, False)):
        yield BranchNode(part)
      else:
        reten = [tuple(l) for l in json.loads(row[RETEN_NAME])]
        reader = HBaseReader(this_node, reten, row[METHOD_NAME])
        yield LeafNode(this_node, reader)

  def _find_paths(self, currNodeRowKey, patterns):
    """
    Recursively generates components
    underneath current_node
    matching the corresponding pattern in patterns
    """
    nodeRow = self._get_row(currNodeRowKey)
    if not nodeRow or len(nodeRow) < 1:
      yield "", {}
    if bool(nodeRow.get(COLUMN_NAME, False)):
      yield currNodeRowKey, {COLUMN_NAME: nodeRow[COLUMN_NAME],
                             RETEN_NAME: nodeRow[RETEN_NAME],
                             METHOD_NAME: nodeRow[METHOD_NAME]}

    if patterns:
      pattern = patterns[0]
      patterns = patterns[1:]
    else:
      pattern = "*"

    subnodes = {}
    for k, v in nodeRow.items():
      search_pattern = "%s:c_" % META_CF_NAME
      len_search = len(search_pattern)
      # branches start with c_
      if k.startswith(search_pattern):
        # pop off <meta_name>:c_ prefix
        key = k[len_search:]
        subnodes[key] = v

    matching_subnodes = match_entries(subnodes.keys(), pattern)
    # we still have more directories to traverse
    if patterns:
      for subnode in matching_subnodes:
        rowKey = subnodes[subnode]
        subNodeContents = self._get_row(rowKey)
        if not subNodeContents:
          continue
        """
        leaves have a cf:NODE column describing their data
        we can't possibly match on a leaf here because we
        have more components in the pattern,
        so only recurse on branches
        """
        search_pattern = "%s:NODE" % META_CF_NAME
        if search_pattern not in subNodeContents.keys():
          for metric, node_list in self._find_paths(rowKey, patterns):
            yield metric, node_list
    else:
      for subnode in matching_subnodes:
        yield subnode, subnodes

  """
  This will break up a list like:
  ['Platform', 'MySQL', '*', '*', '*qps*']
  into
  ['Platform.MySQL', '*', '*', '*qps*']
  Which means fewer gets for each metric.
  Thus...cheaper!
  ['Infrastructure.servers.CH', 'ag*', 'loadavg', '[01][15]']
  In this case, two fewer gets!
  Extrapolate across some of our bigger requests, and this does
  save time.
  """
  def _cheaper_patterns(self, pattern):
    if len(pattern) < 2:
      return pattern
    excluded = ['*', '[', ']', '{', '}', '?']
    current_string = pattern[0]
    chunk = pattern[1]
    del pattern[0]
    while not any(x in chunk for x in excluded):
      current_string += ".%s" % chunk
      del pattern[0]
      try:
        chunk = pattern[0]
      except IndexError:
        break
    final_chunks = [current_string]
    final_chunks.extend(pattern)
    return final_chunks

  def _get_row(self, row):
    try:
      return self.store_table.row(row)
    except Exception:
      return None
