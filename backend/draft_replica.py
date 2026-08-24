"""
draft_replica.py - One replica of the Draft auto-save store.
Run 3 copies, each on its own port:
    python draft_replica.py A 60301
    python draft_replica.py B 60302
    python draft_replica.py C 60303

Each replica accepts writes locally and immediately (available even
if peers are unreachable), then asynchronously gossips the update
to its peers. Conflicting concurrent writes are resolved with
Last-Write-Wins using the Lamport timestamp (same clock discipline
as Experiment 3).
"""
import sys
import time
import threading
from concurrent import futures
import grpc

import artwork_pb2
import artwork_pb2_grpc

ALL_REPLICAS = {
    "A": "localhost:60301",
    "B": "localhost:60302",
    "C": "localhost:60303",
}


class DraftReplica(artwork_pb2_grpc.DraftServiceServicer):
    def __init__(self, name):
        self.name = name
        self.peers = {n: addr for n, addr in ALL_REPLICAS.items() if n != name}
        self.clock = 0
        self.lock = threading.Lock()
        self.store = {}

    def log(self, msg):
        print(f"[{self.name} t={self.clock:>2}] {msg}", flush=True)

    def tick(self):
        with self.lock:
            self.clock += 1
            return self.clock

    def update_clock(self, received_ts):
        with self.lock:
            self.clock = max(self.clock, received_ts) + 1
            return self.clock

    def _apply_if_newer(self, draft_id, content, ts, origin):
        with self.lock:
            current = self.store.get(draft_id)
            if current is None or (ts, origin) > (current[1], current[2]):
                self.store[draft_id] = (content, ts, origin)
                self.log(f"APPLIED '{draft_id}' = \"{content}\" (ts={ts}, origin={origin})")
                return True
            else:
                self.log(f"IGNORED stale update for '{draft_id}' "
                         f"(incoming ts={ts}/{origin} <= current ts={current[1]}/{current[2]})")
                return False

    def _gossip(self, draft_id, content, ts, origin):
        for peer_name, addr in self.peers.items():
            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = artwork_pb2_grpc.DraftServiceStub(channel)
                    stub.SyncUpdate(artwork_pb2.DraftUpdate(
                        draft_id=draft_id, content=content,
                        lamport_timestamp=ts, origin_replica=origin))
                    self.log(f"Gossiped '{draft_id}' -> {peer_name}")
            except grpc.RpcError as e:
                self.log(f"Gossip to {peer_name} failed (will catch up later): {e.code()}")

    def SaveDraft(self, request, context):
        ts = self.tick()
        self._apply_if_newer(request.draft_id, request.content, ts, self.name)
        threading.Thread(
            target=self._gossip,
            args=(request.draft_id, request.content, ts, self.name),
            daemon=True,
        ).start()
        return artwork_pb2.SaveAck(accepted=True, replica=self.name, lamport_timestamp=ts)

    def SyncUpdate(self, request, context):
        self.update_clock(request.lamport_timestamp)
        self._apply_if_newer(request.draft_id, request.content,
                              request.lamport_timestamp, request.origin_replica)
        return artwork_pb2.SaveAck(accepted=True, replica=self.name, lamport_timestamp=self.clock)

    def GetDraft(self, request, context):
        with self.lock:
            entry = self.store.get(request.draft_id)
        if entry is None:
            return artwork_pb2.DraftState(draft_id=request.draft_id, content="", lamport_timestamp=0, origin_replica="")
        content, ts, origin = entry
        return artwork_pb2.DraftState(draft_id=request.draft_id, content=content,
                                       lamport_timestamp=ts, origin_replica=origin)


def serve(name, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    artwork_pb2_grpc.add_DraftServiceServicer_to_server(DraftReplica(name), server)
    server.add_insecure_port(f"localhost:{port}")
    server.start()
    print(f"DraftReplica-{name} listening on localhost:{port}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve(sys.argv[1], int(sys.argv[2]))
