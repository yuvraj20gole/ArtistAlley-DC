"""
load_test.py - Simulates a launch-day traffic surge (many concurrent
users) hitting the fully deployed Docker Compose stack from Exp 10:
  - Artwork browsing  -> artwork-service (Exp 2)
  - Draft auto-saves  -> draft-a/b/c    (Exp 7)
  - Image uploads     -> media-1/2/3, Least-Connections routed (Exp 6)

Reports throughput, latency, and success/failure counts per service,
proving the deployed system holds up under concurrent peak load.
"""
import time
import threading
import random
import grpc

import artwork_pb2
import artwork_pb2_grpc

ARTWORK_ADDR = "localhost:50051"
DRAFT_ADDRS = ["localhost:60301", "localhost:60302", "localhost:60303"]
MEDIA_ADDRS = ["localhost:60201", "localhost:60202", "localhost:60203"]

NUM_ARTWORK_REQUESTS = 30
NUM_DRAFT_SAVES = 15
NUM_MEDIA_UPLOADS = 20

results_lock = threading.Lock()
results = {
    "artwork": {"ok": 0, "fail": 0, "latencies": []},
    "draft": {"ok": 0, "fail": 0, "latencies": []},
    "media": {"ok": 0, "fail": 0, "latencies": []},
}
media_active = [0, 0, 0]
media_lock = threading.Lock()


def record(category, ok, latency):
    with results_lock:
        results[category]["ok" if ok else "fail"] += 1
        results[category]["latencies"].append(latency)


def browse_artwork(i):
    start = time.time()
    try:
        with grpc.insecure_channel(ARTWORK_ADDR) as channel:
            stub = artwork_pb2_grpc.ArtworkServiceStub(channel)
            stub.GetArtwork(
                artwork_pb2.ArtworkRequest(artwork_id=random.choice([1, 2, 3, 4]), lamport_timestamp=0),
                timeout=3,
            )
        record("artwork", True, time.time() - start)
    except grpc.RpcError:
        record("artwork", False, time.time() - start)


def autosave_draft(i):
    addr = random.choice(DRAFT_ADDRS)
    start = time.time()
    try:
        with grpc.insecure_channel(addr) as channel:
            stub = artwork_pb2_grpc.DraftServiceStub(channel)
            stub.SaveDraft(
                artwork_pb2.DraftUpdate(
                    draft_id=f"load-test-draft-{i % 5}",
                    content=f"autosave content #{i}",
                    lamport_timestamp=0,
                    origin_replica="",
                ),
                timeout=3,
            )
        record("draft", True, time.time() - start)
    except grpc.RpcError:
        record("draft", False, time.time() - start)


def pick_least_loaded_media():
    with media_lock:
        idx = media_active.index(min(media_active))
        media_active[idx] += 1
        return idx


def release_media(idx):
    with media_lock:
        media_active[idx] -= 1


def upload_image(i):
    idx = pick_least_loaded_media()
    addr = MEDIA_ADDRS[idx]
    start = time.time()
    try:
        with grpc.insecure_channel(addr) as channel:
            stub = artwork_pb2_grpc.MediaServiceStub(channel)
            stub.ProcessImage(
                artwork_pb2.ImageRequest(image_id=i, lamport_timestamp=0),
                timeout=5,
            )
        record("media", True, time.time() - start)
    except grpc.RpcError:
        record("media", False, time.time() - start)
    finally:
        release_media(idx)


def summarize(name):
    r = results[name]
    total = r["ok"] + r["fail"]
    if total == 0:
        print(f"{name}: no requests")
        return
    lat = r["latencies"]
    avg = sum(lat) / len(lat) if lat else 0
    print(f"{name}: {r['ok']}/{total} succeeded "
          f"({r['fail']} failed), avg latency={avg*1000:.0f}ms, "
          f"max latency={max(lat)*1000:.0f}ms" if lat else f"{name}: no completions")


def main():
    print("=== Simulating launch-day traffic surge on the deployed Docker stack ===")
    print(f"Artwork browsing: {NUM_ARTWORK_REQUESTS} concurrent requests")
    print(f"Draft auto-saves: {NUM_DRAFT_SAVES} concurrent requests")
    print(f"Image uploads:    {NUM_MEDIA_UPLOADS} concurrent requests\n")

    threads = []
    for i in range(NUM_ARTWORK_REQUESTS):
        threads.append(threading.Thread(target=browse_artwork, args=(i,)))
    for i in range(NUM_DRAFT_SAVES):
        threads.append(threading.Thread(target=autosave_draft, args=(i,)))
    for i in range(NUM_MEDIA_UPLOADS):
        threads.append(threading.Thread(target=upload_image, args=(i,)))

    random.shuffle(threads)  # interleave request types, like real concurrent traffic

    start_time = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total_time = time.time() - start_time

    total_requests = NUM_ARTWORK_REQUESTS + NUM_DRAFT_SAVES + NUM_MEDIA_UPLOADS
    total_ok = sum(results[k]["ok"] for k in results)
    total_fail = sum(results[k]["fail"] for k in results)

    print("=== Results ===")
    summarize("artwork")
    summarize("draft")
    summarize("media")
    print(f"\nTotal: {total_ok}/{total_requests} succeeded ({total_fail} failed)")
    print(f"Total wall-clock time: {total_time:.2f}s")
    print(f"Throughput: {total_requests / total_time:.1f} requests/sec")
    print(f"Final media active-connection counts (should be 0,0,0): {media_active}")


if __name__ == "__main__":
    main()
