import time
import threading
from concurrent import futures
import grpc

import artwork_pb2
import artwork_pb2_grpc

ARTWORKS = [
    {"artwork_id": 1, "title": "Sunset Over Ghats", "artist": "Meera Kulkarni",
     "category": "painting", "price": 4500.0, "views": 320, "likes": 45, "status": "active"},
    {"artwork_id": 2, "title": "Neon Dreams", "artist": "Aarav Shah",
     "category": "digital-art", "price": 2800.0, "views": 512, "likes": 88, "status": "active"},
    {"artwork_id": 3, "title": "Terracotta Vase", "artist": "Meera Kulkarni",
     "category": "pottery", "price": 1200.0, "views": 140, "likes": 22, "status": "active"},
    {"artwork_id": 4, "title": "Monsoon Streets", "artist": "Aarav Shah",
     "category": "photography", "price": 3000.0, "views": 275, "likes": 60, "status": "sold"},
]


class LamportClock:
    """Thread-safe Lamport logical clock for the Artwork Service."""
    def __init__(self):
        self._time = 0
        self._lock = threading.Lock()

    def tick(self):
        """Internal event (Lamport Rule 1): increment before any local/send event."""
        with self._lock:
            self._time += 1
            return self._time

    def update(self, received_time):
        """On receiving a message (Lamport Rule 2): max(local, received) + 1."""
        with self._lock:
            self._time = max(self._time, received_time) + 1
            return self._time


clock = LamportClock()


class ArtworkServicer(artwork_pb2_grpc.ArtworkServiceServicer):
    def GetArtwork(self, request, context):
        new_time = clock.update(request.lamport_timestamp)
        print(f"[Server] Received GetArtwork(id={request.artwork_id}, "
              f"client_ts={request.lamport_timestamp}) -> server_lamport_ts={new_time}")

        for art in ARTWORKS:
            if art["artwork_id"] == request.artwork_id:
                send_time = clock.tick()
                return artwork_pb2.ArtworkResponse(lamport_timestamp=send_time, **art)

        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details(f"Artwork {request.artwork_id} not found")
        return artwork_pb2.ArtworkResponse(lamport_timestamp=clock.tick())

    def ListArtworksByCategory(self, request, context):
        new_time = clock.update(request.lamport_timestamp)
        print(f"[Server] Received ListArtworksByCategory(category={request.category}, "
              f"client_ts={request.lamport_timestamp}) -> server_lamport_ts={new_time}")

        for art in ARTWORKS:
            if art["category"] == request.category:
                send_time = clock.tick()
                yield artwork_pb2.ArtworkResponse(lamport_timestamp=send_time, **art)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    artwork_pb2_grpc.add_ArtworkServiceServicer_to_server(ArtworkServicer(), server)
    port = "50051"
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"ArtworkService gRPC server started on port {port} (Lamport clock enabled)")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve()
