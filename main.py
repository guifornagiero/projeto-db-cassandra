from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

cluster = Cluster(
    contact_points=["localhost"], 
    port=9042, 
    auth_provider=PlainTextAuthProvider(username='admin', password='admin')
)

session = cluster.connect()

