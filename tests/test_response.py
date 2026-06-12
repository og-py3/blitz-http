"""
Tests for blitz.response — BlitzResponse field population and accessors.

Covers:
  - All fields set correctly on success and error paths.
  - text property decoding and caching.
  - json() parsing and caching.
  - ok property logic.
  - from_error() factory.
  - BlitzDecodeError on bad encoding / bad JSON.
"""

from __future__ import annotations

import json

import pytest

from blitz.exceptions import BlitzDecodeError, BlitzConnectionError
from blitz.response import BlitzResponse


class TestBlitzResponseFields:
    """Tests for field population."""

    def test_default_fields(self):
        """A default-constructed BlitzResponse has sensible zero values."""
        resp = BlitzResponse()
        assert resp.status == 0
        assert resp.headers == {}
        assert resp.body == b""
        assert resp.url == ""
        assert resp.latency_ms == 0.0
        assert resp.total_ms == 0.0
        assert resp.attempts == 1
        assert resp.from_cache is False
        assert resp.connection_reused is False
        assert resp.error is None
        assert resp.tags == {}
        assert resp.request_method == "GET"

    def test_fields_set_correctly(self):
        """All fields accept and store the provided values."""
        resp = BlitzResponse(
            status=200,
            headers={"content-type": "application/json"},
            body=b'{"key": "value"}',
            url="https://example.com/api",
            latency_ms=12.5,
            total_ms=13.0,
            attempts=2,
            from_cache=False,
            connection_reused=True,
            error=None,
            tags={"job": "scrape"},
            request_method="POST",
        )
        assert resp.status == 200
        assert resp.headers["content-type"] == "application/json"
        assert resp.url == "https://example.com/api"
        assert resp.latency_ms == 12.5
        assert resp.attempts == 2
        assert resp.connection_reused is True
        assert resp.tags == {"job": "scrape"}
        assert resp.request_method == "POST"

    def test_ok_true_for_2xx(self):
        """ok is True for 2xx status codes with no error."""
        for status in (200, 201, 204, 206):
            resp = BlitzResponse(status=status)
            assert resp.ok is True, f"Expected ok=True for {status}"

    def test_ok_false_for_4xx(self):
        """ok is False for 4xx status codes."""
        for status in (400, 401, 403, 404):
            resp = BlitzResponse(status=status)
            assert resp.ok is False, f"Expected ok=False for {status}"

    def test_ok_false_for_5xx(self):
        """ok is False for 5xx status codes."""
        for status in (500, 502, 503, 504):
            resp = BlitzResponse(status=status)
            assert resp.ok is False

    def test_ok_false_when_error_is_set(self):
        """ok is False even for 200 if an error is attached."""
        err = BlitzConnectionError("connection refused")
        resp = BlitzResponse(status=200, error=err)
        assert resp.ok is False


class TestBlitzResponseText:
    """Tests for the text property."""

    def test_text_decodes_utf8(self):
        """text decodes UTF-8 body correctly."""
        resp = BlitzResponse(body="héllo wörld".encode("utf-8"))
        assert resp.text == "héllo wörld"

    def test_text_uses_content_type_charset(self):
        """text respects charset from Content-Type header."""
        body = "Ñoño".encode("latin-1")
        resp = BlitzResponse(
            body=body,
            headers={"content-type": "text/plain; charset=latin-1"},
        )
        assert resp.text == "Ñoño"

    def test_text_is_cached(self):
        """text property returns the same object on repeated access."""
        resp = BlitzResponse(body=b"hello")
        t1 = resp.text
        t2 = resp.text
        assert t1 is t2

    def test_text_raises_decode_error_on_bad_encoding(self):
        """text raises BlitzDecodeError when body cannot be decoded."""
        resp = BlitzResponse(
            body=b"\xff\xfe\xfd",
            headers={"content-type": "text/plain; charset=ascii"},
        )
        with pytest.raises(BlitzDecodeError):
            _ = resp.text


class TestBlitzResponseJson:
    """Tests for the json() method."""

    def test_json_parses_valid_body(self):
        """json() returns the parsed Python object."""
        payload = {"name": "blitz", "version": 1}
        resp = BlitzResponse(body=json.dumps(payload).encode())
        assert resp.json() == payload

    def test_json_returns_list(self):
        """json() handles JSON arrays."""
        resp = BlitzResponse(body=b"[1, 2, 3]")
        assert resp.json() == [1, 2, 3]

    def test_json_is_cached(self):
        """json() caches the parsed result so the body is parsed only once."""
        resp = BlitzResponse(body=b'{"x": 1}')
        r1 = resp.json()
        r2 = resp.json()
        assert r1 is r2

    def test_json_raises_decode_error_on_invalid_json(self):
        """json() raises BlitzDecodeError for non-JSON bodies."""
        resp = BlitzResponse(body=b"not json at all {{{")
        with pytest.raises(BlitzDecodeError):
            resp.json()

    def test_json_raises_on_empty_body(self):
        """json() raises BlitzDecodeError on an empty body."""
        resp = BlitzResponse(body=b"")
        with pytest.raises(BlitzDecodeError):
            resp.json()


class TestBlitzResponseFromError:
    """Tests for the from_error() factory method."""

    def test_from_error_sets_error_field(self):
        """from_error() stores the provided BlitzError on the response."""
        err = BlitzConnectionError("timeout", url="https://example.com")
        resp = BlitzResponse.from_error(err, url="https://example.com", attempts=3)
        assert resp.error is err
        assert resp.attempts == 3
        assert resp.status == 0

    def test_from_error_ok_is_false(self):
        """Responses created via from_error() always have ok=False."""
        err = BlitzConnectionError("error")
        resp = BlitzResponse.from_error(err)
        assert resp.ok is False

    def test_from_error_preserves_tags(self):
        """from_error() forwards tags to the response."""
        err = BlitzConnectionError("err")
        tags = {"source": "batch_job"}
        resp = BlitzResponse.from_error(err, tags=tags)
        assert resp.tags == tags

    def test_repr_includes_error(self):
        """repr() mentions the error when one is present."""
        err = BlitzConnectionError("refused")
        resp = BlitzResponse.from_error(err)
        assert "error=" in repr(resp)

    def test_repr_includes_status_on_success(self):
        """repr() shows the status code on a successful response."""
        resp = BlitzResponse(status=200, latency_ms=5.3)
        r = repr(resp)
        assert "200" in r
        assert "5.3" in r
