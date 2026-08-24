"""
draft_client.py - Simulates concurrent auto-saves from two devices
hitting different replicas for the SAME draft, then checks that all
replicas eventually converge to one consistent value.
"""
import sys
import time
import threading
import grpc

import artwork_pb2
import artwork_pb2_grpc

REPLICAS = {
    "A": "localhost:60301",
    "B": "localhost:60302",
    "C": "localhost:60303",
}


def save(replica_name, draft_id, content):
    addr = REPLICAS[replica_name]
    with grpc.insecure_channel(addr) as channel:
        stub = artwork_pb2_grpc.DraftServiceStub(channel)
        ack = stub.SaveDraft(artwork_pb2.DraftUpdate(
            draft_id=draft_id, content=content, lamport_timestamp=0, origin_replica=""))
        print(f"[Client] Saved on Replica-{replica_name}: \"{content}\" "
              f"-> accepted={ack.accepted}, replica_ts={ack.lamport_timestamp}")


def read(replica_name, draft_id):
    addr = REPLICAS[replica_name]
    with grpc.insecure_channel(addr) as channel:
        stub = artwork_pb2_grpc.DraftServiceStub(channel)
        state = stub.GetDraft(artwork_pb2.DraftQuery(draft_id=draft_id))
        return state.content, state.lamport_timestamp, state.origin_replica


def main():
    draft_id = "artwork-42-draft"
    print("=== Simulating two devices auto-saving the SAME draft on DIFFERENT replicas ===\n")

    t1 = threading.Thread(target=save, args=("A", draft_id, "Sunset over ghats - v1 (Device 1, Replica A)"))
    t2 = threading.Thread(target=save, args=("C", draft_id, "Sunset over ghats - v2 (Device 2, Replica C)"))
    t1.start()
    time.sleep(0.05)
    t2.start()
    t1.join()
    t2.join()

    print("\n=== Immediately after writes (replicas may still be mid-gossip) ===")
    for name in REPLICAS:
        content, ts, origin = read(name, draft_id)
        print(f"Replica-{name}: \"{content}\" (ts={ts}, origin={origin})")

    print("\nWaiting 2s for gossip to finish propagating...\n")
    time.sleep(2)

    print("=== After convergence window ===")
    results = {}
    for name in REPLICAS:
        content, ts, origin = read(name, draft_id)
        results[name] = (content, ts, origin)
        print(f"Replica-{name}: \"{content}\" (ts={ts}, origin={origin})")

    values = set(r[0] for r in results.values())
    if len(values) == 1:
        print(f"\n*** CONVERGED: all 3 replicas agree on: \"{values.pop()}\" ***")
    else:
        print(f"\n*** NOT YET CONVERGED: {values} ***")


if __name__ == "__main__":
    main()
