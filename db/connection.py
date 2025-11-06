import os
import psycopg2
from urllib.parse import quote_plus

def _build_dsn(host: str, port: str, dbname: str, user: str, password: str, sslmode: str) -> str:
    """Build a safe libpq DSN from parts (URL‑quote user/password)."""
    user_q = quote_plus(user) if user is not None else ""
    pw_q = quote_plus(password) if password is not None else ""
    return f"postgresql://{user_q}:{pw_q}@{host}:{port}/{dbname}?sslmode={sslmode}"

def get_connection():
    """
    Connect using SUPABASE_DB_* environment variables only.

    Required env vars:
      SUPABASE_DB_HOST
      SUPABASE_DB_NAME
      SUPABASE_DB_USER
      SUPABASE_DB_PASSWORD

    Optional:
      SUPABASE_DB_PORT (defaults to 5432)
      SUPABASE_SSLMODE (defaults to 'require')
    """
    host = os.getenv("SUPABASE_DB_HOST")
    port = os.getenv("SUPABASE_DB_PORT", "5432")
    dbname = os.getenv("SUPABASE_DB_NAME")
    user = os.getenv("SUPABASE_DB_USER")
    password = os.getenv("SUPABASE_DB_PASSWORD")
    sslmode = os.getenv("SUPABASE_SSLMODE", "require")

    missing = [k for k, v in (
        ("SUPABASE_DB_HOST", host),
        ("SUPABASE_DB_NAME", dbname),
        ("SUPABASE_DB_USER", user),
        ("SUPABASE_DB_PASSWORD", password),
    ) if not v]
    if missing:
        raise RuntimeError(
            "Missing Supabase DB configuration: " + ", ".join(missing) +
            ". Set SUPABASE_DB_* config vars."
        )

    dsn = _build_dsn(host, port, dbname, user, password, sslmode)
    # psycopg2 accepts the DSN string; it will honor sslmode in the query string.
    return psycopg2.connect(dsn)
