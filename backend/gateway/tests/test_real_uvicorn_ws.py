import asyncio
import socket
import threading

import pytest
import uvicorn
import websockets
from meetmind_gateway.app import create_app
from meetmind_gateway.config import GatewaySettings
from meetmind_schemas.events import WSCloseCode


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.asyncio
async def test_real_uvicorn_ws_handshake_succeeds_then_receives_4401():
    """CO-2 Proof: Verify against a REAL uvicorn server that handshake succeeds (101)
    and then the websockets client receives RFC 6455 close frame with code 4401.
    """
    port = get_free_port()
    settings = GatewaySettings(
        neon_database_url="postgresql://localhost/test",
        redis_url="redis://localhost:6379/0",
        app_env="production",
        allow_token_in_dev=False,
    )
    app = create_app(settings)
    config = uvicorn.Config(app=app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait for uvicorn server to start
    for _ in range(50):
        if server.started:
            break
        await asyncio.sleep(0.05)

    try:
        uri = f"ws://127.0.0.1:{port}/ws/ingest/real_uvicorn_test"

        # Connect with the REAL websockets client (no token, no ticket provided)
        # 1. Handshake HTTP 101 MUST succeed (proving ws.accept() called)
        # 2. Immediately after handshake, the server sends close frame 4401
        async with websockets.connect(uri) as ws:
            # If server sent HTTP 403, websockets.connect would raise InvalidStatus.
            # Because handshake succeeded, we are inside the context manager!
            with pytest.raises(websockets.exceptions.ConnectionClosed) as exc_info:
                await ws.recv()

            close_code = (
                exc_info.value.rcvd.code
                if hasattr(exc_info.value, "rcvd") and exc_info.value.rcvd
                else exc_info.value.code
            )
            assert close_code == WSCloseCode.UNAUTHORIZED
            assert close_code == 4401
            print(
                f"VERIFIED: Real uvicorn WS handshake succeeded, close code received: {close_code}"
            )
    finally:
        server.should_exit = True
        thread.join(timeout=3.0)
