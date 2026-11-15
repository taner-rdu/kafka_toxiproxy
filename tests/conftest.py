import time
import uuid

import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.compose import DockerCompose

BOOTSTRAP = "localhost:9092"


def _wait_for_broker(timeout: float = 120.0) -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            admin.list_topics(timeout=2)
            return
        except Exception:
            time.sleep(1)
    raise TimeoutError(f"Kafka was not ready after {timeout}s")


@pytest.fixture(scope="session")
def kafka_environment(request):
    """Start docker-compose.yml for the test session. Return the bootstrap address.

    The fixture removes the containers and their messages when the session ends.
    Run `just down` first if the environment already runs.
    """
    compose = DockerCompose(
        request.config.rootpath, compose_file_name="docker-compose.yml", wait=False
    )
    compose.start()
    try:
        _wait_for_broker()
        yield BOOTSTRAP
    finally:
        compose.stop()


@pytest.fixture
def isolated_topic():
    """Create a new topic with 3 partitions for one test. Delete it afterwards.

    Needs a running Kafka on localhost:9092 (run `just up`).
    """
    name = f"test-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    for future in admin.create_topics(
        [NewTopic(name, num_partitions=3, replication_factor=1)]
    ).values():
        future.result(timeout=30)

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        partitions = admin.list_topics(name, timeout=5).topics[name].partitions
        if len(partitions) == 3 and all(p.leader >= 0 for p in partitions.values()):
            break
        time.sleep(0.5)

    yield name

    for future in admin.delete_topics([name]).values():
        try:
            future.result(timeout=30)
        except Exception:
            pass
