"""
media_server.py - A single Media Service backend (artwork image
upload/processing: resize, thumbnail generation, metadata extraction).
Run several copies on different ports, e.g.:
    python media_server.py 60201
    python media_server.py 60202
    python media_server.py 60203
"""
import sys
import time
import random
from concurrent import futures
import grpc

import artwork_pb2
import artwork_pb2_grpc


class MediaServicer(artwork_pb2_grpc.MediaServiceServicer):
    def __init__(self, name):
        self.name = name

    def ProcessImage(self, request, context):
        # Simulate variable image-processing time (larger/higher-res
        # artwork uploads genuinely take longer to resize + thumbnail
        # than smaller ones) so connection counts differ meaningfully.
        work_time = random.uniform(0.5, 2.5)
        print(f"[{self.name}] Processing image {request.image_id} "
              f"(will take {work_time:.1f}s)")
        time.sleep(work_time)
        print(f"[{self.name}] Finished image {request.image_id}")
        return artwork_pb2.ImageReply(
            image_id=request.image_id,
            processed_by=self.name,
            status="processed",
            lamport_timestamp=request.lamport_timestamp + 1,
        )


def serve(port):
    name = f"MediaServer-{port}"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    artwork_pb2_grpc.add_MediaServiceServicer_to_server(MediaServicer(name), server)
    server.add_insecure_port(f"localhost:{port}")
    server.start()
    print(f"{name} listening on localhost:{port}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve(int(sys.argv[1]))
