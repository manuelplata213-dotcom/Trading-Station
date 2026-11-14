import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from config import settings
from exchange_client import ExchangeClient
from execution_manager import ExecutionManager
from strategy_engine import QuantitativeStrategyEngine

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("quant.server")

exchange_client = ExchangeClient()
strategy_engine = QuantitativeStrategyEngine()
execution_manager = ExecutionManager(exchange_client, strategy_engine)

security = HTTPBearer(auto_error=False)


def _create_token(username: str) -> str:
    expiry = int(time.time()) + 86400
    payload = f"{username}:{expiry}"
    sig = hmac.new(settings.JWT_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}:{sig}"


def _verify_token(token: str) -> bool:
    try:
        parts = token.split(":")
        if len(parts) != 3:
            return False
        username, expiry_str, sig = parts
        if int(expiry_str) < int(time.time()):
            return False
        expected = hmac.new(
            settings.JWT_SECRET.encode(), f"{username}:{expiry_str}".encode(), hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(sig, expected) and username == settings.OWNER_USERNAME
    except Exception:
        return False


async def require_auth(
    request: Request,
    creds: HTTPAuthorizationCredentials = Depends(security),
) -> str:
    token = creds.credentials if creds else request.query_params.get("token", "")
    if not token or not _verify_token(token):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    return settings.OWNER_USERNAME


async def _execution_loop() -> None:
    while True:
        try:
            await execution_manager.step()
        except Exception as exc:
            logger.error(json.dumps({"event": "loop_step_error", "error": str(exc)}))
        await asyncio.sleep(1.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not os.getenv("OWNER_PASSWORD"):
        logger.warning(
            json.dumps({
                "event": "ephemeral_admin_password_generated",
                "username": settings.OWNER_USERNAME,
                "password": settings.OWNER_PASSWORD,
            })
        )
    await exchange_client.initialize()
    await execution_manager.init_persistence()
    task = asyncio.create_task(_execution_loop())
    yield
    task.cancel()
    await exchange_client.close()


app = FastAPI(title="Institutional Quant Execution Engine", lifespan=lifespan)


class LoginRequest(BaseModel):
    username: str
    password: str


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard() -> HTMLResponse:
    html_path = Path(__file__).parent / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.post("/api/auth/login")
async def login(body: LoginRequest):
    valid_user = hmac.compare_digest(body.username, settings.OWNER_USERNAME)
    valid_pass = hmac.compare_digest(body.password, settings.OWNER_PASSWORD)
    if not (valid_user and valid_pass):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    return {"token": _create_token(body.username), "expires_in": 86400}


@app.get("/api/telemetry/stream")
async def telemetry_stream(request: Request, _user: str = Depends(require_auth)) -> StreamingResponse:
    async def event_generator() -> AsyncGenerator[str, None]:
        while True:
            if await request.is_disconnected():
                break
            payload = json.dumps(execution_manager.get_telemetry_snapshot())
            yield f"data: {payload}\n\n"
            await asyncio.sleep(1.0)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


@app.post("/api/control/start")
async def control_start(_user: str = Depends(require_auth)):
    execution_manager.start_engine()
    return {"status": "RUNNING", "state": execution_manager.state.value}


@app.post("/api/control/pause")
async def control_pause(_user: str = Depends(require_auth)):
    execution_manager.pause_engine()
    return {"status": "PAUSED", "state": execution_manager.state.value}


@app.post("/api/control/kill")
async def control_kill(_user: str = Depends(require_auth)):
    result = await execution_manager.emergency_kill_switch("Operator triggered dashboard kill switch")
    return result


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
