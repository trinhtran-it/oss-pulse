from __future__ import annotations

import gzip
import json
import shutil
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:  # pragma: no cover
    pytest.skip("Java is required for Spark tests", allow_module_level=True)

from spark.flatten_events import build_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    # The JVM runs in a non-UTC zone on purpose: results must still be UTC.
    builder = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=America/Los_Angeles")
    )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    session = build_spark()  # applies the job's own settings to the same session
    yield session
    session.stop()


def write_ndjson_gz(path: Path, lines: list[dict | str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt") as f:
        for line in lines:
            f.write(line if isinstance(line, str) else json.dumps(line))
            f.write("\n")
    return path


def event(event_id: str, event_type: str, created_at: str, **overrides) -> dict:
    e = {
        "id": event_id,
        "type": event_type,
        "actor": {"id": 1, "login": "alice", "display_login": "alice"},
        "repo": {"id": 10, "name": "acme/widgets", "url": "https://api.github.com/repos/acme/w"},
        "payload": {},
        "public": True,
        "created_at": created_at,
    }
    e.update(overrides)
    return e
