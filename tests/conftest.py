import time

import pytest
from confluent_kafka.admin import AdminClient
from testcontainers.compose import DockerCompose

BOOTSTRAP = "localhost:9092"
TOPIC = "test"


def _wait_for_topic(timeout: float = 120.0) -> None:
    """Wait until the broker answers and init-topics has created the topic."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if TOPIC in admin.list_topics(timeout=2).topics:
                return
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError(f"Topic '{TOPIC}' was not ready after {timeout}s")


@pytest.fixture(scope="session")
def kafka(request):
    """Start docker-compose.yml for the test session. Return the bootstrap address.

    The fixture removes the containers and their messages when the session ends.
    Run `just down` first if the environment already runs.
    """
    compose = DockerCompose(
        request.config.rootpath, compose_file_name="docker-compose.yml", wait=False
    )
    compose.start()
    try:
        _wait_for_topic()
        yield BOOTSTRAP
    finally:
        compose.stop()
