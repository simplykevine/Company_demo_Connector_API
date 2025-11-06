import re
import os
import logging
from typing import Any, Dict
from fastapi import FastAPI, Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from dotenv import load_dotenv
import uvicorn

# load local .env for development; on Heroku/Prod use Config Vars instead
load_dotenv()

# app
app = FastAPI(title="Supabase Company API")

# logger setup
logger = logging.getLogger("main")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)

# keys from environment (set these in Heroku Config Vars or your environment)
ADMIN_KEY = os.getenv("ADMIN_API_KEY")
USER_KEY = os.getenv("USER_API_KEY")


def mask_key(value: str) -> str:
    if not value:
        return "<missing>"
    return f"****{value[-4:]}"


def check_auth(request: Request, required_key: str, role_name: str):
    auth_header = request.headers.get("Authorization")
    if auth_header != f"Bearer {required_key}":
        raise HTTPException(status_code=403, detail=f"Invalid {role_name} API key")
    return role_name


class NormalizePathMiddleware(BaseHTTPMiddleware):
    """
    Collapse multiple consecutive slashes in the request path into a single slash.
    Example: //admin/query -> /admin/query
    """
    async def dispatch(self, request: Request, call_next):
        path = request.scope.get("path", "")
        normalized = re.sub(r"/{2,}", "/", path)
        if normalized != path:
            client = request.client.host if request.client else "<unknown>"
            logger.info("Normalized path: %s -> %s from %s", path, normalized, client)
            request.scope["path"] = normalized
        return await call_next(request)


app.add_middleware(NormalizePathMiddleware)


@app.get("/")
async def root():
    return {"status": "ok", "message": "Supabase Company API running"}


@app.get("/health")
async def health():
    """
    Lightweight health check. Attempts a minimal DB connection check if db.connection.get_connection exists.
    The DB check is best-effort: errors are included in the response but do not raise a server exception.
    """
    db_status: Dict[str, Any] = {"connected": False}
    try:
        # import lazily to avoid import-time DB work when not needed
        from db.connection import get_connection  # type: ignore
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
        conn.close()
        db_status["connected"] = True
    except Exception as e:
        logger.exception("Database health check failed")
        # include a short error message for debugging, avoid leaking credentials
        db_status["error"] = str(e).splitlines()[0][:200]
    return {"status": "ok", "db": db_status}


async def _parse_json_body(request: Request) -> Dict[str, Any]:
    try:
        data = await request.json()
        if not isinstance(data, dict):
            raise HTTPException(status_code=400, detail="Request JSON must be an object")
        return data
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid or missing JSON body")


def _log_request_for_debug(request: Request, data: Dict[str, Any]):
    auth = request.headers.get("Authorization", "")
    masked = mask_key(auth.replace("Bearer ", ""))
    client = request.client.host if request.client else "<unknown>"
    logger.info("Request %s %s from %s auth=%s has_sql=%s",
                request.method, request.url.path, client, masked, "sql" in data)


# Routes
@app.post("/user/query")
async def user_query(request: Request):
    role = check_auth(request, USER_KEY, "user")
    data = await _parse_json_body(request)
    _log_request_for_debug(request, data)

    sql = data.get("sql")
    if not sql:
        raise HTTPException(status_code=400, detail="Missing 'sql' in request JSON")

    try:
        from db.query_tool import query_company_db  # local import to fail fast if missing
        results = query_company_db(sql)
        return {"status": "success", "rows": len(results), "results": results, "role": role}
    except HTTPException:
        raise
    except Exception:
        logger.exception("user_query failed")
        # Do not leak DB internals to clients
        raise HTTPException(status_code=500, detail="Database error")


@app.post("/admin/query")
async def admin_query(request: Request):
    role = check_auth(request, ADMIN_KEY, "admin")
    data = await _parse_json_body(request)
    _log_request_for_debug(request, data)

    sql = data.get("sql")
    if not sql:
        raise HTTPException(status_code=400, detail="Missing 'sql' in request JSON")

    try:
        from db.query_tool import query_admin_db  # local import to fail fast if missing
        results = query_admin_db(sql)
        return {"status": "success", "rows": len(results), "results": results, "role": role}
    except HTTPException:
        raise
    except Exception:
        logger.exception("admin_query failed")
        raise HTTPException(status_code=500, detail="Database error")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
