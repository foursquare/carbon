try:
  from hashlib import md5
except ImportError:
  from md5 import md5
import bisect
from binascii import crc32

def md5_key(key):
    big_hash = md5( str(key) ).hexdigest()
    return int(big_hash[:4], 16)

class ConsistentHashRing:
  def __init__(self, nodes, replica_count=100, hash_type=None):
    self.ring = []
    self.nodes = set()
    self.replica_count = replica_count
    self.hash_type = hash_type

    for node in nodes:
      self.add_node(node)

    # Precompute hash function
    if self.hash_type in (None, 'md5'):
        self.hash_function = md5_key
    elif self.hash_type == 'crc32':
        # The md5 is dropped into the range of 0xffff. Let's
        # make sure crc is too
        self.hash_function = lambda key : (crc32(key) & 0xffff)
    elif self.hash_type == 'hash':
        # The md5 is dropped into the range of 0xffff. Let's
        # make sure crc is too
        self.hash_function = lambda key : (hash(key) & 0xffff)
    else:
        raise Exception('Unsupported hash type: %s' % self.hash_type)

  def compute_ring_position(self, key):
    return self.hash_function(key)

  def add_node(self, node):
    self.nodes.add(node)
    for i in range(self.replica_count):
      replica_key = "%s:%d" % (node, i)
      position = self.compute_ring_position(replica_key)
      entry = (position, node)
      bisect.insort(self.ring, entry)

  def remove_node(self, node):
    self.nodes.discard(node)
    self.ring = [entry for entry in self.ring if entry[1] != node]

  def get_node(self, key):
    assert self.ring
    node = None
    node_iter = self.get_nodes(key)
    node = node_iter.next()
    node_iter.close()
    return node

  def get_nodes(self, key):
    assert self.ring
    nodes = set()
    position = self.compute_ring_position(key)
    search_entry = (position, None)
    index = bisect.bisect_left(self.ring, search_entry) % len(self.ring)
    last_index = (index - 1) % len(self.ring)
    while len(nodes) < len(self.nodes) and index != last_index:
      next_entry = self.ring[index]
      (position, next_node) = next_entry
      if next_node not in nodes:
        nodes.add(next_node)
        yield next_node

      index = (index + 1) % len(self.ring)
