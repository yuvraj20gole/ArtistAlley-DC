"""
proctor_server.py - A single Proctoring Server backend.
Run several copies on different ports, e.g.:
    python proctor_server.py 60201
    python proctor_server.py 60202
    python proctor_server.py 60203
"""
import sys
import time
import random
from concurrent import futures
import grpc

import artwork_pb2
import artwork_pb2_grpc


class ProctorServicer(artwork_pb2_grpc.ProctorServiceServicer):
    def __init__(self, name):
        self.name = name

    def VerifySubmission(self, request, context):
        # Simulate variable verification work (some submissions take longer
        # to proctor/verify than others) so connection counts genuinely differ.
        work_time = random.uniform(0.5, 2.5)
        print(f"[{self.name}] Verifying submission {request.submission_id} "
              f"(will take {work_time:.1f}s)")
        time.sleep(work_time)
        print(f"[{self.name}] Finished submission {request.submission_id}")
        return artwork_pb2.VerifyReply(
            submission_id=request.submission_id,
            verified_by=self.name,
            status="verified",
            lamport_timestamp=request.lamport_timestamp + 1,
        )


def serve(port):
    name = f"ProctorServer-{port}"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    artwork_pb2_grpc.add_ProctorServiceServicer_to_server(ProctorServicer(name), server)
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
