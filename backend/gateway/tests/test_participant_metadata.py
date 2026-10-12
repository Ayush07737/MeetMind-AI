"""Unit tests for P0-3: Optional participant metadata validation and session hash storage."""

import json
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from meetmind_gateway.app import create_app
from meetmind_gateway.auth import AuthenticatedUser
from meetmind_gateway.session_manager import SessionManager


@pytest.fixture
def mock_authenticator():
    mock = AsyncMock()
    mock.authenticate.return_value = AuthenticatedUser(
        user_id="user_meta_123",
        tenant_id="tenant_meta_abc",
        session_id="sess_123",
        claims={"org_id": "org_meta_abc", "org_role": "org:admin"},
    )
    return mock


@pytest.fixture
def app_and_client(mock_authenticator):
    app = create_app()
    app.state.authenticator = mock_authenticator

    mock_redis = AsyncMock()
    mock_redis.hset = AsyncMock()
    mock_redis.expire = AsyncMock()
    app.state.session_manager = SessionManager(mock_redis)
    app.state.redis = mock_redis

    client = TestClient(app)
    return client, mock_redis


def test_create_meeting_with_valid_metadata(app_and_client):
    client, mock_redis = app_and_client
    payload = {
        "local_user_name": "Alice Smith",
        "attendees": ["Bob Jones", "Charlie Brown"],
    }
    resp = client.post(
        "/v1/meetings",
        json=payload,
        headers={"Authorization": "Bearer valid_token"},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["local_user_name"] == "Alice Smith"
    assert data["attendees"] == ["Bob Jones", "Charlie Brown"]

    # Verify session hash mapping written to Redis
    mock_redis.hset.assert_called_once()
    _, kwargs = mock_redis.hset.call_args
    mapping = kwargs.get("mapping", {})
    assert mapping["local_user_name"] == "Alice Smith"
    assert json.loads(mapping["attendees"]) == ["Bob Jones", "Charlie Brown"]


def test_create_meeting_without_metadata(app_and_client):
    client, mock_redis = app_and_client
    resp = client.post(
        "/v1/meetings",
        json={},
        headers={"Authorization": "Bearer valid_token"},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert "local_user_name" not in data
    assert "attendees" not in data

    _, kwargs = mock_redis.hset.call_args
    mapping = kwargs.get("mapping", {})
    assert "local_user_name" not in mapping
    assert "attendees" not in mapping


def test_local_user_name_too_long(app_and_client):
    client, _ = app_and_client
    payload = {"local_user_name": "A" * 101}
    resp = client.post(
        "/v1/meetings",
        json=payload,
        headers={"Authorization": "Bearer valid_token"},
    )
    assert resp.status_code == 422


def test_local_user_name_not_string(app_and_client):
    client, _ = app_and_client
    payload = {"local_user_name": 12345}
    resp = client.post(
        "/v1/meetings",
        json=payload,
        headers={"Authorization": "Bearer valid_token"},
    )
    assert resp.status_code == 422


def test_attendees_too_many(app_and_client):
    client, _ = app_and_client
    payload = {"attendees": [f"User {i}" for i in range(51)]}
    resp = client.post(
        "/v1/meetings",
        json=payload,
        headers={"Authorization": "Bearer valid_token"},
    )
    assert resp.status_code == 422


def test_attendees_element_too_long(app_and_client):
    client, _ = app_and_client
    payload = {"attendees": ["A" * 101]}
    resp = client.post(
        "/v1/meetings",
        json=payload,
        headers={"Authorization": "Bearer valid_token"},
    )
    assert resp.status_code == 422


def test_attendees_element_not_string(app_and_client):
    client, _ = app_and_client
    payload = {"attendees": ["Valid", 999]}
    resp = client.post(
        "/v1/meetings",
        json=payload,
        headers={"Authorization": "Bearer valid_token"},
    )
    assert resp.status_code == 422


def test_attendees_not_list(app_and_client):
    client, _ = app_and_client
    payload = {"attendees": "not a list"}
    resp = client.post(
        "/v1/meetings",
        json=payload,
        headers={"Authorization": "Bearer valid_token"},
    )
    assert resp.status_code == 422
