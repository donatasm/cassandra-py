import sys

sys.path.append('gen-py')
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from cassandra import Cassandra
from cassandra.ttypes import ColumnParent
from cassandra.ttypes import SlicePredicate
from cassandra.ttypes import SliceRange
from cassandra.ttypes import KeyRange
from cassandra.ttypes import ConsistencyLevel

host, port = '127.0.0.1', 9160
keyspace = 'ks1'
column_family = 'products'
count = 1000

transport = TTransport.TFramedTransport(TSocket.TSocket(host, port))
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Cassandra.Client(protocol)
transport.open()
client.set_keyspace(keyspace)
column_parent = ColumnParent(column_family=column_family)
slice_range = SliceRange(start='', finish='', count=count)
slice_predicate = SlicePredicate(slice_range=slice_range)
key_range = KeyRange(start_key='', end_key='')
consistency_level = ConsistencyLevel.ONE
slices = client.get_range_slices(column_parent, slice_predicate, key_range, consistency_level)
transport.close()
print(slices)