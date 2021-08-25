from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.graph_traversal import unfold, addV, outV, inV, values, outE
from gremlin_python.process.traversal import P
from gremlin_python.process.traversal import Cardinality
from gremlin_python.structure.graph import Graph


class GremlinWrapper:
    def __init__(self):
        graph = Graph()
        self.remoteConn = DriverRemoteConnection(
            'wss://neptune-instance-1.cua5qo31vmsf.us-east-1.neptune.amazonaws.com:8182/gremlin', 'g')
        self.g = graph.traversal().withRemote(self.remoteConn)

    def create_vertex(self, vertex_label, vertex_name):
        # default database cardinality is used when Cardinality argument is not specified
        return self.g.V().has(vertex_label, 'name', vertex_name).fold().coalesce(unfold(), addV(vertex_label).property('name', vertex_name)). \
            next()

    def create_sp_artist(self, artist_name, sp_id, sp_popularity, sp_n_followers, sp_genres):
        return self.g.V().has('artist', 'name', artist_name).fold().coalesce(unfold(), addV('artist').
            property('name', artist_name)).property('sp_id', sp_id).property('sp_popularity', sp_popularity). \
            property('sp_n_followers', sp_n_followers).property('sp_genres', str(sp_genres)).next()

    def add_property_to_artist(self, artist_name, property_dict):
        for prop in property_dict.keys():
            self.g.V().has('name', artist_name).property(prop, property_dict[prop]).toList()

    def get_vertex(self, vertex_label, vertex_name):
        return self.g.V(). \
            has(vertex_label, 'name', vertex_name). \
            toList()

    def create_edge(self, v_out, v_in, label_):
        return self.g.V().has('name', v_out).addE(label_). \
            to(self.g.V().has('name', v_in).next()).next()

    def get_vertices(self, label_=None):
        if label_:
            return self.g.V().hasLabel(label_).name.toList()
        elif self.g.V().count().toList()[0]:
            return self.g.V().name.toList()
        else:
            return

    def get_edges(self, label_=None):
        if label_:
            return self.g.V().bothE(label_).project('from', 'to').by(outV().name).by(inV().name).toList()
        else:
            return self.g.V().bothE().project('from', 'to').by(outV().name).by(inV().name).toList()

    def get_sp_populars(self, low_limit=0, high_limit=None):
        # return the artists with spotify popularity within low_limit and high_limit
        if high_limit:
            return self.g.V().filter(values('sp_popularity').unfold().is_(P.gte(low_limit)).and_().
                                     values('sp_popularity').unfold().is_(P.lte(high_limit))).name.toList()
        else:
            return self.g.V().filter(values('sp_popularity').unfold().is_(P.gte(low_limit))).name.toList()

    def get_leaves(self):
        if self.g.V().count().toList()[0]:
            return self.g.V().filter(outE().count().is_(0)).sp_id.toList()
        else:
            return ["1Xyo4u8uXC1ZmMpatF05PJ", "1uNFoZAHBGtllmzznpCI3s", "6M2wZ9GZgrQXHCFfjv46we", "66CXWjxzNUsdJxJ2JdwvnR",
                    "1McMsnEElThX1knmY4oliG", "1vyhD5VmyZ7KMfW5gqLgo5", "0du5cEVh5yTK9QJze8zA0C", "4r63FhuTkUYltbVAg5TQnk",
                    "5cj0lLjcoR7YOSnhnX0Po5", "04gDigrS5kc9YWfZHwBETP", "6eUKZXaKkcviH0Ku9w2n3V", "1Cs0zKBU1kc0i8ypK3B9ai",
                    "3TVXtAsR1Inumwj472S9r4", "6qqNVTkY8uBg9cP3Jd7DAH", "4q3ewBCX7sLwd24euuV69X", "6LuN9FCkKOj5PcnpouEgny",
                    "7jVv8c5Fj3E9VhNjxT4snq", "64KEffDW9EtZ1y2vBYgq8T", "0Y5tJX1MQlPlqiwlOH1tJY", "5pKCCKE2ajJHZ9KAiaK11H"]

    def get_spotify_id_dict(self):
        res = {x['sp_id']: 'sp_id' for x in self.g.V().hasLabel('artist').project('sp_id').by('sp_id').toList()}
        if res:
            return res
        else:
            return {}

    def __del__(self):
        self.remoteConn.close()

