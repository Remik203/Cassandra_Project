from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra import ConsistencyLevel

_cluster = None
_session = None

def connect():
    global _cluster, _session
    policy = DCAwareRoundRobinPolicy(local_dc='dc1')
    _cluster = Cluster(
        contact_points=['127.0.0.1'],
        port=9042,
        load_balancing_policy=policy
    )
    _session = _cluster.connect()

    _session.set_keyspace('libertyn') 

    _session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
    print("[INFO] Cassandra connected.")

def get_session():
    if _session is None:
        raise Exception("The session has not been initialized.")
    return _session

def close():
    if _cluster:
        _cluster.shutdown()
        print("[INFO] Cassandra disconnected.")