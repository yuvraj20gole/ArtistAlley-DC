"""
raft_node.py - Raft leader election over gRPC, coordinating which
replica is the active Lock Manager (reusing Experiment 5's lock
logic). Run 3 copies:
    python raft_node.py A 60401
    python raft_node.py B 60402
    python raft_node.py C 60403

Only the current LEADER accepts AcquireLock/ReleaseLock from
clients. If the leader crashes, the remaining followers detect the
missed heartbeats via randomized election timeouts and elect a new
leader automatically.
"""
import sys
import time
import random
import threading
from concurrent import futures
import grpc

import artwork_pb2
import artwork_pb2_grpc

ALL_NODES = {
    "A": "localhost:60401",
    "B": "localhost:60402",
    "C": "localhost:60403",
}

HEARTBEAT_INTERVAL = 0.5          # leader sends heartbeats this often
ELECTION_TIMEOUT_RANGE = (1.5, 3.0)   # followers wait a random time in this range before starting an election


class RaftNode(artwork_pb2_grpc.RaftServiceServicer, artwork_pb2_grpc.LockServiceServicer):
    def __init__(self, node_id):
        self.id = node_id
        self.peers = {n: addr for n, addr in ALL_NODES.items() if n != node_id}
        self.lock = threading.Lock()

        self.state = "FOLLOWER"       # FOLLOWER -> CANDIDATE -> LEADER
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        self.last_heartbeat = time.time()

        # Reused from Experiment 5: only meaningful while this node is LEADER
        self.locks = {"draft": None, "submission": None}

    def log(self, msg):
        print(f"[{self.id} term={self.current_term} {self.state}] {msg}", flush=True)

    def _reset_election_timer(self):
        self.last_heartbeat = time.time()

    def _random_timeout(self):
        return random.uniform(*ELECTION_TIMEOUT_RANGE)

    # ---------- Raft RPC handlers ----------
    def RequestVote(self, request, context):
        with self.lock:
            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
                self.state = "FOLLOWER"

            grant = (request.term >= self.current_term and
                     self.voted_for in (None, request.candidate_id))
            if grant:
                self.voted_for = request.candidate_id
                self._reset_election_timer()
                self.log(f"Voted for {request.candidate_id} (term {request.term})")
            return artwork_pb2.VoteReply(term=self.current_term, vote_granted=grant)

    def AppendEntries(self, request, context):
        """Used purely as a heartbeat here (no log replication needed
        for this experiment's scope - leader election + failover is
        the focus)."""
        with self.lock:
            if request.term >= self.current_term:
                self.current_term = request.term
                self.state = "FOLLOWER"
                self.leader_id = request.leader_id
                self._reset_election_timer()
                return artwork_pb2.HeartbeatReply(term=self.current_term, success=True)
            return artwork_pb2.HeartbeatReply(term=self.current_term, success=False)

    # ---------- Lock Manager RPCs (Experiment 5 logic, LEADER-only) ----------
    def AcquireLock(self, request, context):
        with self.lock:
            if self.state != "LEADER":
                hint = self.leader_id or "unknown"
                return artwork_pb2.LockReply(granted=False, message=f"not-leader,try={hint}")
            owner = self.locks.get(request.resource_id)
            if owner is None:
                self.locks[request.resource_id] = request.holder_id
                self.log(f"GRANTED '{request.resource_id}' to Node-{request.holder_id}")
                return artwork_pb2.LockReply(granted=True, message="granted")
            return artwork_pb2.LockReply(granted=False, message=f"held by Node-{owner}")

    def ReleaseLock(self, request, context):
        with self.lock:
            if self.locks.get(request.resource_id) == request.holder_id:
                self.locks[request.resource_id] = None
                self.log(f"RELEASED '{request.resource_id}'")
            return artwork_pb2.LockReply(granted=True, message="released")

    # ---------- Background loops ----------
    def _election_watchdog(self):
        """Followers/Candidates: if too long since last heartbeat/vote
        activity, start a new election."""
        while True:
            timeout = self._random_timeout()
            time.sleep(0.1)
            elapsed = time.time() - self.last_heartbeat
            while elapsed < timeout:
                time.sleep(0.1)
                if self.state == "LEADER":
                    break
                elapsed = time.time() - self.last_heartbeat
            if self.state != "LEADER" and elapsed >= timeout:
                self._start_election()

    def _start_election(self):
        with self.lock:
            self.state = "CANDIDATE"
            self.current_term += 1
            self.voted_for = self.id
            term = self.current_term
            self._reset_election_timer()
        self.log("Starting election")

        votes = 1  # vote for self
        for peer_id, addr in self.peers.items():
            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = artwork_pb2_grpc.RaftServiceStub(channel)
                    reply = stub.RequestVote(
                        artwork_pb2.VoteRequest(term=term, candidate_id=self.id), timeout=1)
                    if reply.vote_granted:
                        votes += 1
                    elif reply.term > term:
                        with self.lock:
                            self.current_term = reply.term
                            self.state = "FOLLOWER"
                        return
            except grpc.RpcError:
                self.log(f"No response from {peer_id} (may be down)")

        with self.lock:
            if self.state == "CANDIDATE" and votes > len(ALL_NODES) // 2:
                self.state = "LEADER"
                self.leader_id = self.id
                self.log(f"*** ELECTED LEADER *** ({votes}/{len(ALL_NODES)} votes)")
            else:
                self.state = "FOLLOWER"
                self.log(f"Election failed ({votes}/{len(ALL_NODES)} votes) - reverting to follower")

    def _heartbeat_loop(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            if self.state != "LEADER":
                continue
            for peer_id, addr in self.peers.items():
                try:
                    with grpc.insecure_channel(addr) as channel:
                        stub = artwork_pb2_grpc.RaftServiceStub(channel)
                        stub.AppendEntries(
                            artwork_pb2.HeartbeatRequest(term=self.current_term, leader_id=self.id),
                            timeout=1)
                except grpc.RpcError:
                    pass  # peer may be down; will catch up when it returns


def serve(node_id):
    node = RaftNode(node_id)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    artwork_pb2_grpc.add_RaftServiceServicer_to_server(node, server)
    artwork_pb2_grpc.add_LockServiceServicer_to_server(node, server)
    addr = ALL_NODES[node_id]
    port = addr.split(":")[1]
    server.add_insecure_port(f"localhost:{port}")
    server.start()
    print(f"RaftNode-{node_id} listening on {addr}")

    threading.Thread(target=node._election_watchdog, daemon=True).start()
    threading.Thread(target=node._heartbeat_loop, daemon=True).start()

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve(sys.argv[1])
