"""
load_balancer.py - Least Connections load balancer in front of N
Media Service backends. Simulates M incoming artwork image uploads
arriving concurrently and routes each to the backend with the
fewest currently-active (in-flight) connections.
"""
import threading
import time
import grpc

import artwork_pb2
import artwork_pb2_grpc

BACKENDS = ["localhost:60201", "localhost:60202", "localhost:60203"]
NUM_REQUESTS = 9

active = [0] * len(BACKENDS)
lock = threading.Lock()
clock = 0
clock_lock = threading.Lock()


def tick():
    global clock
    with clock_lock:
        clock += 1
        return clock


def pick_least_connections():
    """Core Least Connections strategy: choose the backend with the
    fewest active connections right now; increment it immediately
    so the very next pick sees the updated count."""
    with lock:
        idx = active.index(min(active))
        active[idx] += 1
        print(f"[LB] Routing -> {BACKENDS[idx]} "
              f"(active connections now: {active})")
        return idx


def release(idx):
    with lock:
        active[idx] -= 1
        print(f"[LB] Released {BACKENDS[idx]} "
              f"(active connections now: {active})")


def handle_request(image_id):
    idx = pick_least_connections()
    addr = BACKENDS[idx]
    ts = tick()
    try:
        with grpc.insecure_channel(addr) as channel:
            stub = artwork_pb2_grpc.MediaServiceStub(channel)
            reply = stub.ProcessImage(
                artwork_pb2.ImageRequest(image_id=image_id, lamport_timestamp=ts))
            print(f"[LB] Image {image_id} -> processed by {reply.processed_by}")
    finally:
        release(idx)


def main():
    print(f"[LB] Load Balancer starting. Backends: {BACKENDS}")
    print(f"[LB] Dispatching {NUM_REQUESTS} artwork image uploads using Least Connections...\n")

    threads = []
    for img_id in range(1, NUM_REQUESTS + 1):
        t = threading.Thread(target=handle_request, args=(img_id,))
        threads.append(t)
        t.start()
        time.sleep(0.15)  # stagger arrivals slightly, like real traffic

    for t in threads:
        t.join()

    print("\n[LB] All image uploads processed.")
    print(f"[LB] Final active connection counts (should all be 0): {active}")


if __name__ == "__main__":
    main()
