"""
net_config.py - Resolves service hostnames for BOTH local runs
(localhost, as in Exp 2-8) and Docker Compose runs (container
service names). Set RUN_ENV=docker to switch modes; defaults to
local so nothing we already built breaks.
"""
import os

RUN_ENV = os.environ.get("RUN_ENV", "local")

LOCAL_HOSTS = {
    "artwork-service": "localhost:50051",
    "media-1": "localhost:60201",
    "media-2": "localhost:60202",
    "media-3": "localhost:60203",
    "lock-manager": "localhost:60100",
    "draft-a": "localhost:60301",
    "draft-b": "localhost:60302",
    "draft-c": "localhost:60303",
    "raft-a": "localhost:60401",
    "raft-b": "localhost:60402",
    "raft-c": "localhost:60403",
    "redis": "localhost:6379",
}

DOCKER_HOSTS = {
    "artwork-service": "artwork-service:50051",
    "media-1": "media-1:60201",
    "media-2": "media-2:60202",
    "media-3": "media-3:60203",
    "lock-manager": "lock-manager:60100",
    "draft-a": "draft-a:60301",
    "draft-b": "draft-b:60302",
    "draft-c": "draft-c:60303",
    "raft-a": "raft-a:60401",
    "raft-b": "raft-b:60402",
    "raft-c": "raft-c:60403",
    "redis": "redis:6379",
}


def resolve(name):
    hosts = DOCKER_HOSTS if RUN_ENV == "docker" else LOCAL_HOSTS
    return hosts[name]
