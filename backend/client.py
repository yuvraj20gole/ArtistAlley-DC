import grpc

import artwork_pb2
import artwork_pb2_grpc


def run():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = artwork_pb2_grpc.ArtworkServiceStub(channel)

        print("--- Unary RPC: GetArtwork(artwork_id=2) ---")
        response = stub.GetArtwork(artwork_pb2.ArtworkRequest(artwork_id=2))
        print(f"{response.title} by {response.artist} | Rs.{response.price} | "
              f"views={response.views} likes={response.likes}")

        print("\n--- Server-streaming RPC: ListArtworksByCategory('painting') ---")
        for art in stub.ListArtworksByCategory(artwork_pb2.CategoryRequest(category="painting")):
            print(f"[{art.artwork_id}] {art.title} - Rs.{art.price} ({art.status})")

        print("\n--- Error handling: GetArtwork(artwork_id=999) ---")
        try:
            stub.GetArtwork(artwork_pb2.ArtworkRequest(artwork_id=999))
        except grpc.RpcError as e:
            print(f"RPC failed as expected: {e.code()} - {e.details()}")


if __name__ == "__main__":
    run()
