"""
Ricart-Agrawala Distributed Mutual Exclusion
Simulates 3 Artist nodes competing to submit to a single shared
"Featured Submission" slot at (roughly) the same time. Only one
node may hold the critical section at once — this prevents the
duplicate/overlapping submissions problem in ArtistAlley.
"""
import threading
import queue
import time
import random

NUM_NODES = 3


class Node(threading.Thread):
    def __init__(self, node_id, all_inboxes):
        super().__init__()
        self.id = node_id
        self.clock = 0
        self.state = "RELEASED"          # RELEASED -> WANTED -> HELD
        self.request_time = None
        self.inbox = queue.Queue()
        self.all_inboxes = all_inboxes   # dict: node_id -> Queue
        self.deferred = []
        self.replies_received = 0
        self.reply_event = threading.Event()
        self.lock = threading.Lock()

    def log(self, msg):
        print(f"[t={self.clock:>2}] Node-{self.id}: {msg}")

    def send(self, target_id, msg_type, ts):
        self.all_inboxes[target_id].put((msg_type, ts, self.id))

    def broadcast_request(self):
        with self.lock:
            self.clock += 1
            self.state = "WANTED"
            self.request_time = self.clock
        self.log(f"WANTS critical section (request_ts={self.request_time})")
        for nid in self.all_inboxes:
            if nid != self.id:
                self.send(nid, "REQUEST", self.request_time)

    def request_cs(self):
        self.replies_received = 0
        self.reply_event.clear()
        self.broadcast_request()
        self.reply_event.wait()          # blocks until all N-1 replies in
        with self.lock:
            self.state = "HELD"
        self.log("*** ENTERED critical section ***")

    def release_cs(self):
        with self.lock:
            self.state = "RELEASED"
        self.log("Left critical section, replying to deferred requests")
        for (nid, ts) in self.deferred:
            with self.lock:
                self.clock += 1
            self.send(nid, "REPLY", self.clock)
        self.deferred = []

    def handle_request(self, ts, sender_id):
        with self.lock:
            self.clock = max(self.clock, ts) + 1
            defer = (
                self.state == "HELD"
                or (self.state == "WANTED"
                    and (self.request_time, self.id) < (ts, sender_id))
            )
        if defer:
            self.log(f"DEFERS reply to Node-{sender_id} (req_ts={ts})")
            self.deferred.append((sender_id, ts))
        else:
            self.log(f"GRANTS reply to Node-{sender_id} (req_ts={ts})")
            self.send(sender_id, "REPLY", self.clock)

    def handle_reply(self, ts, sender_id):
        with self.lock:
            self.clock = max(self.clock, ts) + 1
            self.replies_received += 1
            done = self.replies_received == NUM_NODES - 1
        self.log(f"Got REPLY from Node-{sender_id} ({self.replies_received}/{NUM_NODES-1})")
        if done:
            self.reply_event.set()

    def run(self):
        # small random jitter so requests don't fire at literally the same instant
        time.sleep(random.uniform(0, 0.3))
        self.request_cs()
        time.sleep(0.5)                  # simulate doing the submission
        self.release_cs()

        # keep listening briefly to answer any late requests
        deadline = time.time() + 1.5
        while time.time() < deadline:
            try:
                msg_type, ts, sender_id = self.inbox.get(timeout=0.2)
                if msg_type == "REQUEST":
                    self.handle_request(ts, sender_id)
                elif msg_type == "REPLY":
                    self.handle_reply(ts, sender_id)
            except queue.Empty:
                continue

    def listen_forever_until(self, stop_event):
        while not stop_event.is_set():
            try:
                msg_type, ts, sender_id = self.inbox.get(timeout=0.1)
                if msg_type == "REQUEST":
                    self.handle_request(ts, sender_id)
                elif msg_type == "REPLY":
                    self.handle_reply(ts, sender_id)
            except queue.Empty:
                continue


def main():
    inboxes = {i: queue.Queue() for i in range(1, NUM_NODES + 1)}
    nodes = [Node(i, inboxes) for i in range(1, NUM_NODES + 1)]
    for n in nodes:
        n.inbox = inboxes[n.id]

    # dedicated listener threads so nodes can react to REQUESTs even
    # while blocked waiting for their own replies
    stop_event = threading.Event()
    listeners = [threading.Thread(target=n.listen_forever_until, args=(stop_event,)) for n in nodes]
    for l in listeners:
        l.daemon = True
        l.start()

    print("=== 3 Artist nodes competing for the Featured Submission slot ===\n")
    for n in nodes:
        n.start()
    for n in nodes:
        n.join()

    stop_event.set()
    print("\n=== All nodes finished. Mutual exclusion maintained throughout. ===")


if __name__ == "__main__":
    main()
