import time
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


class ArtworkServicer(artwork_pb2_grpc.ArtworkServiceServicer):
    def GetArtwork(self, request, context):
        for art in ARTWORKS:
            if art["artwork_id"] == request.artwork_id:
                return artwork_pb2.ArtworkResponse(**art)
        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details(f"Artwork {request.artwork_id} not found")
        return artwork_pb2.ArtworkResponse()

    def ListArtworksByCategory(self, request, context):
        for art in ARTWORKS:
            if art["category"] == request.category:
                yield artwork_pb2.ArtworkResponse(**art)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    artwork_pb2_grpc.add_ArtworkServiceServicer_to_server(ArtworkServicer(), server)
    port = "50051"
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"ArtworkService gRPC server started on port {port}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve()
