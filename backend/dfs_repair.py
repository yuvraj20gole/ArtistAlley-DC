"""
dfs_repair.py - Restores the replication factor after a storage node dies
(Experiment 11: Distributed File System).

It asks the metadata server which chunks have fewer live replicas than the
replication factor, copies each one from a live replica to a live node that
does not hold it yet (checking the checksum on the way), and tells the
metadata server about the new copy.
"""
import hashlib
import grpc

import artwork_pb2
import artwork_pb2_grpc
import net_config

REPLICATION_FACTOR = 2


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def repair():
    try:
        with grpc.insecure_channel(net_config.resolve("dfs-meta")) as channel:
            meta = artwork_pb2_grpc.DfsMetadataServiceStub(channel)
            status = meta.ClusterStatus(artwork_pb2.DfsStatusRequest(), timeout=5)

            alive = {n.node_id: n.address for n in status.nodes if n.alive}
            print(f"Live nodes: {', '.join(sorted(alive)) or 'none'}")
            copied = 0
            problems = 0

            for f in status.files:
                for chunk in sorted(f.chunks, key=lambda c: c.index):
                    holders = [(nid, addr) for nid, addr, up in
                               zip(chunk.node_ids, chunk.addresses, chunk.alive) if up]
                    missing = REPLICATION_FACTOR - len(holders)
                    if missing <= 0:
                        continue
                    if not holders:
                        print(f"  {f.filename} chunk {chunk.index}: NO live replica, "
                              f"cannot repair")
                        problems += 1
                        continue

                    # read a good copy from a live holder
                    data = None
                    for nid, addr in holders:
                        try:
                            with grpc.insecure_channel(addr) as sch:
                                reply = artwork_pb2_grpc.DfsStorageServiceStub(sch).GetChunk(
                                    artwork_pb2.DfsChunkId(chunk_id=chunk.chunk_id),
                                    timeout=3)
                            if sha256(reply.data) == chunk.sha256:
                                data = reply.data
                                break
                        except grpc.RpcError:
                            continue
                    if data is None:
                        print(f"  {f.filename} chunk {chunk.index}: no healthy copy "
                              f"to read from")
                        problems += 1
                        continue

                    targets = [n for n in sorted(alive) if n not in chunk.node_ids]
                    for target in targets[:missing]:
                        try:
                            with grpc.insecure_channel(alive[target]) as sch:
                                ack = artwork_pb2_grpc.DfsStorageServiceStub(sch).PutChunk(
                                    artwork_pb2.DfsChunkData(
                                        chunk_id=chunk.chunk_id, data=data,
                                        sha256=chunk.sha256), timeout=5)
                            if not ack.ok:
                                continue
                            meta.AddReplica(artwork_pb2.DfsReplicaUpdate(
                                filename=f.filename, chunk_index=chunk.index,
                                node_id=target), timeout=5)
                            print(f"  {f.filename} chunk {chunk.index}: copied "
                                  f"to {target}")
                            copied += 1
                        except grpc.RpcError as e:
                            print(f"  {f.filename} chunk {chunk.index}: could not "
                                  f"copy to {target} ({e.code().name})")
                            problems += 1

            print(f"Repair finished: {copied} chunk copy(ies) made, "
                  f"{problems} problem(s).")
    except grpc.RpcError as e:
        print(f"Could not reach the metadata server ({e.code().name}).")


if __name__ == "__main__":
    repair()
