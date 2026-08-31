"""
cache_gateway.py - Cache-aside gateway in front of the Artwork
Service (server.py from Experiment 2). Adds Redis caching with two
fault-tolerance paths:
  1. Redis unreachable -> transparently fall back to querying the
     database (Artwork Service) directly. Client never sees an error.
  2. Database (Artwork Service) unreachable, but a cached copy
     exists (even if TTL-expired and evicted-then-kept as a stale
     backup) -> serve the stale cached copy rather than failing.
"""
import json
import time
import grpc
import redis

import artwork_pb2
import artwork_pb2_grpc

REDIS_HOST = "localhost"
REDIS_PORT = 6379
CACHE_TTL_SECONDS = 10          # short TTL so cache hits/misses are easy to observe
ARTWORK_SERVICE_ADDR = "localhost:50051"

STALE_KEY_PREFIX = "stale:artwork:"   # kept indefinitely as a fallback, never expires
FRESH_KEY_PREFIX = "fresh:artwork:"   # normal cache entry, expires after CACHE_TTL_SECONDS


def get_redis():
    """Fresh connection per call so we can independently detect if
    Redis has gone down without holding a stale broken client."""
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_connect_timeout=1, socket_timeout=1)


def fetch_from_database(artwork_id):
    """Direct call to the Exp 2 Artwork Service (the 'database')."""
    with grpc.insecure_channel(ARTWORK_SERVICE_ADDR) as channel:
        stub = artwork_pb2_grpc.ArtworkServiceStub(channel)
        response = stub.GetArtwork(
            artwork_pb2.ArtworkRequest(artwork_id=artwork_id, lamport_timestamp=0),
            timeout=2,
        )
        return {
            "artwork_id": response.artwork_id,
            "title": response.title,
            "artist": response.artist,
            "price": response.price,
            "status": response.status,
        }


def get_artwork(artwork_id):
    fresh_key = f"{FRESH_KEY_PREFIX}{artwork_id}"
    stale_key = f"{STALE_KEY_PREFIX}{artwork_id}"

    # --- Try Redis first ---
    try:
        r = get_redis()
        cached = r.get(fresh_key)
        if cached:
            print(f"[Gateway] CACHE HIT for artwork {artwork_id}")
            return json.loads(cached), "cache-hit"
        print(f"[Gateway] CACHE MISS for artwork {artwork_id} -- querying database")
    except redis.exceptions.RedisError as e:
        print(f"[Gateway] REDIS UNAVAILABLE ({e}) -- falling back to database directly")
        r = None

    # --- Cache miss (or Redis down): go to the database ---
    try:
        data = fetch_from_database(artwork_id)
        print(f"[Gateway] Fetched artwork {artwork_id} from DATABASE")
        if r is not None:
            try:
                r.set(fresh_key, json.dumps(data), ex=CACHE_TTL_SECONDS)
                r.set(stale_key, json.dumps(data))   # no expiry - fault-tolerance backup
            except redis.exceptions.RedisError:
                print("[Gateway] Redis write failed, continuing without caching this result")
        return data, "database"

    except grpc.RpcError as e:
        # --- Database unreachable: try to serve a stale cached copy ---
        print(f"[Gateway] DATABASE UNAVAILABLE ({e.code()}) -- checking for stale cache")
        if r is not None:
            try:
                stale = r.get(stale_key)
                if stale:
                    print(f"[Gateway] Serving STALE cached copy for artwork {artwork_id}")
                    return json.loads(stale), "stale-fallback"
            except redis.exceptions.RedisError:
                pass
        raise RuntimeError(f"Artwork {artwork_id} unavailable: both database and cache failed")


if __name__ == "__main__":
    import sys
    artwork_id = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    data, source = get_artwork(artwork_id)
    print(f"\nResult (source={source}): {data}")
