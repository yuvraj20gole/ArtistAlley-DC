import sys
import time
import grpc

import artwork_pb2
import artwork_pb2_grpc


def acquire(stub, resource, holder, ts):
    print(f"Node-{holder}: requesting lock on '{resource}'")
    reply = stub.AcquireLock(artwork_pb2.LockRequest(resource_id=resource, holder_id=holder, timestamp=ts))
    print(f"Node-{holder}: '{resource}' -> granted={reply.granted} ({reply.message})")
    return reply.granted


def release(stub, resource, holder):
    stub.ReleaseLock(artwork_pb2.LockRequest(resource_id=resource, holder_id=holder, timestamp=0))
    print(f"Node-{holder}: released '{resource}'")


def run(node_id, first, second):
    ts = node_id
    with grpc.insecure_channel("localhost:60100") as channel:
        stub = artwork_pb2_grpc.LockServiceStub(channel)
        held = []

        if acquire(stub, first, node_id, ts):
            held.append(first)

        time.sleep(1.5)  # simulate work, widens the deadlock window

        ok = acquire(stub, second, node_id, ts)
        if not ok:
            print(f"Node-{node_id}: ABORTED on '{second}' -- releasing held locks and retrying")
            for r in held:
                release(stub, r, node_id)
            held = []
            time.sleep(1)
            if acquire(stub, second, node_id, ts):
                held.append(second)
                if acquire(stub, first, node_id, ts):
                    held.append(first)
        else:
            held.append(second)

        time.sleep(0.5)
        for r in held:
            release(stub, r, node_id)
        print(f"Node-{node_id}: DONE")


if __name__ == "__main__":
    node_id = int(sys.argv[1])
    role = sys.argv[2]
    if role == "autosave":
        run(node_id, "draft", "submission")
    else:
        run(node_id, "submission", "draft")
