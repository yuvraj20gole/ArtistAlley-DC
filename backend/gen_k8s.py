import os

IMAGE = "artistalley-backend:latest"


def deployment(name, port, command=None, image=IMAGE, replicas=1):
    lines = [
        "apiVersion: apps/v1",
        "kind: Deployment",
        "metadata:",
        f"  name: {name}",
        "spec:",
        f"  replicas: {replicas}",
        "  selector:",
        "    matchLabels:",
        f"      app: {name}",
        "  template:",
        "    metadata:",
        "      labels:",
        f"        app: {name}",
        "    spec:",
        "      containers:",
        f"        - name: {name}",
        f"          image: {image}",
    ]
    if image == IMAGE:
        lines.append("          imagePullPolicy: Never")
    if command:
        items = ", ".join(f'"{c}"' for c in command)
        lines.append(f"          command: [{items}]")
    lines += [
        "          ports:",
        f"            - containerPort: {port}",
    ]
    return "\n".join(lines) + "\n"


def service(name, port):
    return "\n".join([
        "apiVersion: v1",
        "kind: Service",
        "metadata:",
        f"  name: {name}",
        "spec:",
        "  selector:",
        f"    app: {name}",
        "  ports:",
        f"    - port: {port}",
        f"      targetPort: {port}",
    ]) + "\n"


components = [
    ("redis", 6379, None, "redis:7-alpine", 1),
    ("artwork-service", 50051, ["python", "server.py"], IMAGE, 1),
    ("media", 60201, ["python", "media_server.py", "60201"], IMAGE, 3),
    ("lock-manager", 60100, ["python", "lock_manager.py", "detect"], IMAGE, 1),
    ("draft-a", 60301, ["python", "draft_replica.py", "A", "60301"], IMAGE, 1),
    ("draft-b", 60302, ["python", "draft_replica.py", "B", "60302"], IMAGE, 1),
    ("draft-c", 60303, ["python", "draft_replica.py", "C", "60303"], IMAGE, 1),
    ("raft-a", 60401, ["python", "raft_node.py", "A"], IMAGE, 1),
    ("raft-b", 60402, ["python", "raft_node.py", "B"], IMAGE, 1),
    ("raft-c", 60403, ["python", "raft_node.py", "C"], IMAGE, 1),
]

docs = []
for name, port, command, image, replicas in components:
    docs.append(deployment(name, port, command, image, replicas))
    svc = service(name, port)
    if name == "media":
        svc = svc.replace("spec:\n", "spec:\n  clusterIP: None\n", 1)
    docs.append(svc)

os.makedirs("k8s", exist_ok=True)
with open("k8s/artistalley.yaml", "w") as f:
    f.write("---\n".join(docs))

print("Wrote k8s/artistalley.yaml with", len(components), "components")
