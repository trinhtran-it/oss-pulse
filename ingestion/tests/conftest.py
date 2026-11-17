"""Fakes for HTTP and GCS so tests run offline and without credentials."""

from __future__ import annotations

import io
import threading
from collections.abc import Iterable

import pytest
import requests

from ingestion.gharchive import Config


class FakeRaw(io.BytesIO):
    decode_content = True


class FakeResponse:
    def __init__(self, status: int, body: bytes = b"", content_length: int | None = None):
        self.status_code = status
        size = len(body) if content_length is None else content_length
        self.headers = {"Content-Length": str(size)}
        self.raw = FakeRaw(body)
        self.closed = False

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *exc: object) -> None:
        self.closed = True


class FakeSession:
    """Returns queued outcomes per URL: a FakeResponse, or an exception to raise."""

    def __init__(self, script: dict[str, Iterable[FakeResponse | Exception]] | None = None):
        self._script = {url: list(items) for url, items in (script or {}).items()}
        self.calls: list[str] = []
        self._lock = threading.Lock()

    def get(self, url: str, **kwargs: object) -> FakeResponse:
        with self._lock:
            self.calls.append(url)
            outcome = self._script[url].pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def close(self) -> None:
        pass


class FakeBlob:
    def __init__(self, bucket: FakeBucket, name: str):
        self._bucket, self.name = bucket, name

    @property
    def size(self) -> int | None:
        data = self._bucket.objects.get(self.name)
        return None if data is None else len(data)

    def upload_from_file(self, file_obj, content_type=None, **kwargs) -> None:
        assert file_obj.tell() == 0, "resumable uploads require the stream at position 0"
        chunks = []
        while chunk := file_obj.read(4):  # small reads: exercise chunked streaming
            chunks.append(chunk)
        self._bucket.objects[self.name] = b"".join(chunks)
        self._bucket.uploads.append((self.name, content_type, kwargs))

    def delete(self) -> None:
        self._bucket.objects.pop(self.name)


class FakeBucket:
    def __init__(self, name: str = "test-bucket"):
        self.name = name
        self.objects: dict[str, bytes] = {}
        self.uploads: list[tuple[str, str | None, dict]] = []

    def blob(self, blob_name: str, chunk_size: int | None = None) -> FakeBlob:
        return FakeBlob(self, blob_name)

    def get_blob(self, blob_name: str) -> FakeBlob | None:
        return FakeBlob(self, blob_name) if blob_name in self.objects else None


@pytest.fixture
def config() -> Config:
    return Config(bucket="test-bucket", base_url="https://example.test", max_attempts=3)


@pytest.fixture
def bucket() -> FakeBucket:
    return FakeBucket()


@pytest.fixture
def no_sleep() -> list[float]:
    """Collects requested sleeps instead of sleeping."""
    return []
