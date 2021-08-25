import GremlinWrapper as gw
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
import json
import time


def crawl_spotify(ids, n_artists=100):

    with open('spotify_api_cred.json') as f:
        creds = json.load(f)
        auth_manager = SpotifyClientCredentials(client_id=creds['client_id'],
                                                client_secret=creds['client_secret'])
        sp = spotipy.Spotify(auth_manager=auth_manager)
    visited = {}
    queue = []
    added_artists = G.get_spotify_id_dict()
    for id_ in ids:
        queue.append(id_)

    for id_ in queue:
        artist = sp.artist(id_)
        G.create_sp_artist(artist['name'], artist['id'], artist['popularity'], artist['followers']['total'],
                           artist['genres'])
    id_ = queue[0]
    visited[id_] = True

    start_ = time.time()

    while queue and len(visited) < n_artists:
        id_ = queue.pop(0)
        related_artists = sp.artist_related_artists(id_)['artists']
        for artist in related_artists:
            if artist['id'] not in added_artists:
                G.create_sp_artist(artist['name'], artist['id'], artist['popularity'], artist['followers']['total'], artist['genres'])
                G.create_edge(sp.artist(id_)['name'], artist['name'], 'sp_fansAlsoLike')
                added_artists[artist['id']] = 'sp_id'
            if artist['id'] not in visited:
                visited[artist['id']] = True
                queue.append(artist['id'])
                if not len(visited)%1000:
                    print(f"{len(visited)} artists are added, {time.time()-start_} has elapsed")
    return G


if __name__ == '__main__':
    start = time.time()
    n_artists = 500000
    G = gw.GremlinWrapper()
    crawl_spotify(G.get_leaves(), n_artists)
    print(f"Crawling spotify for {n_artists} artists took {time.time()-start} seconds")