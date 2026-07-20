import grpc

import artwork_pb2
import artwork_pb2_grpc


class LamportClock:
    """Lamport logical clock for the Recommendation Service (client side)."""
    def __init__(self):
        self._time = 0

    def tick(self):
        self._time += 1
        return self._time

    def update(self, received_time):
        self._time = max(self._time, received_time) + 1
        return self._time


clock = LamportClock()


def run():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = artwork_pb2_grpc.ArtworkServiceStub(channel)

        print("--- Unary RPC: GetArtwork(artwork_id=2) ---")
        send_ts = clock.tick()
        print(f"[Client] Sending request with lamport_ts={send_ts}")
        response = stub.GetArtwork(
            artwork_pb2.ArtworkRequest(artwork_id=2, lamport_timestamp=send_ts))
        new_ts = clock.update(response.lamport_timestamp)
        print(f"[Client] Received response with server_ts={response.lamport_timestamp} "
              f"-> client_lamport_ts={new_ts}")
        print(f"{response.title} by {response.artist} | Rs.{response.price}")

        print("\n--- Server-streaming RPC: ListArtworksByCategory('painting') ---")
        send_ts = clock.tick()
        print(f"[Client] Sending request with lamport_ts={send_ts}")
        for art in stub.ListArtworksByCategory(
                artwork_pb2.CategoryRequest(category="painting", lamport_timestamp=send_ts)):
            new_ts = clock.update(art.lamport_timestamp)
            print(f"[Client] Received '{art.title}' with server_ts={art.lamport_timestamp} "
                  f"-> client_lamport_ts={new_ts}")

        print("\n--- Error handling: GetArtwork(artwork_id=999) ---")
        send_ts = clock.tick()
        try:
            stub.GetArtwork(
                artwork_pb2.ArtworkRequest(artwork_id=999, lamport_timestamp=send_ts))
        except grpc.RpcError as e:
            print(f"RPC failed as expected: {e.code()} - {e.details()}")

        print(f"\nFinal client Lamport clock value: {clock._time}")


if __name__ == "__main__":
    run()
