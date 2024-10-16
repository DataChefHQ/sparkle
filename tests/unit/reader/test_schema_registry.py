import json

import pytest
import responses

from sparkle.reader.schema_registry import SchemaRegistry

TEST_SCHEMA_REGISTRY = "http://test-schema-registry:8081"
TEST_TOPIC = "test"
TEST_SCHEMA = {
    "type": "record",
    "name": "test",
    "fields": [{"name": "test", "type": "string"}],
}


@pytest.fixture
def schema_api_response():
    """Fixture providing a mock schema response from the schema registry API."""
    return {
        "subject": "test-value",
        "version": 1,
        "id": 1,
        "schema": json.dumps({"properties": TEST_SCHEMA}),
    }


@responses.activate
def test_fetch_schema(schema_api_response):
    """Test fetching the schema from the schema registry without authentication."""
    responses.add(
        responses.GET,
        f"{TEST_SCHEMA_REGISTRY}/subjects/{TEST_TOPIC}-value/versions/latest",
        json=schema_api_response,
        status=200,
    )

    schema = SchemaRegistry(TEST_SCHEMA_REGISTRY).fetch_schema("test")

    assert json.loads(schema) == {
        "properties": {
            "type": "record",
            "name": "test",
            "fields": [{"name": "test", "type": "string"}],
        }
    }


@responses.activate
def test_fetch_schema_with_auth(schema_api_response):
    """Test fetching the schema with basic authentication headers."""
    responses.add(
        responses.GET,
        f"{TEST_SCHEMA_REGISTRY}/subjects/{TEST_TOPIC}-value/versions/latest",
        json=schema_api_response,
        status=200,
    )

    SchemaRegistry(TEST_SCHEMA_REGISTRY, username="test", password="test").fetch_schema(
        "test"
    )
    assert responses.calls[0].request.headers["Authorization"] == "Basic dGVzdDp0ZXN0"


@responses.activate
def test_fetch_cached_schema(schema_api_response):
    """Test that the schema is fetched only once and cached on subsequent requests."""
    url = f"{TEST_SCHEMA_REGISTRY}/subjects/{TEST_TOPIC}-value/versions/latest"
    responses.add(
        responses.GET,
        url,
        json=schema_api_response,
        status=200,
    )

    schema_registry = SchemaRegistry(TEST_SCHEMA_REGISTRY)
    schema_registry.cached_schema("test")
    schema_registry.cached_schema("test")

    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == url


@responses.activate
def test_put_schema(schema_api_response):
    """Test registering a schema to the schema registry and returning the schema ID."""
    responses.add(
        responses.POST,
        f"{TEST_SCHEMA_REGISTRY}/subjects/{TEST_TOPIC}-value/versions",
        json={"id": 1},
        status=200,
    )

    schema_id = SchemaRegistry(TEST_SCHEMA_REGISTRY).put_schema(
        "test", json.dumps(TEST_SCHEMA)
    )

    assert schema_id == 1
