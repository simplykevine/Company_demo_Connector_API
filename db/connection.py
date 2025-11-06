import os
import psycopg2
from urllib.parse import urlparse, quote_plus

def _build_dsn_from_parts(host, port, dbname, user, password, sslmode):
    """
    Build a libpq-style DSN string from individual parts. We URL-quote the password
    and username to be safe with special characters.
    """
    user_quoted = quote_plus(user) if user is not None else ""
    password_quoted = quote_plus(password) if password is not None else ""
    return f"postgresql://{user_quoted}:{password_quoted}@{host}:{port}/{dbname}?sslmode={sslmode}"

def get_connection():
    """
    Return a psycopg2 connection.

    Priority:
      1) DATABASE_URL (full DSN) if present (Heroku Postgres / other DSN).
      2) SUPABASE_DB_* env vars (useful for Supabase connections).
      3) DB_* env vars (local dev fallback).

    Raises RuntimeError with a clear message if required settings are missing.
    """
    # 1) DATABASE_URL (Heroku Postgres or other DSN)
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        sslmode = os.getenv("DB_SSLMODE", os.getenv("SUPABASE_SSLMODE", "require"))
        # psycopg2 accepts a DSN string; pass sslmode explicitly too for clarity.
        return psycopg2.connect(database_url, sslmode=sslmode)

    # 2) SUPABASE_DB_* (explicit Supabase environment variables)
    sup_host = os.getenv("SUPABASE_DB_HOST")
    if sup_host:
        sup_port = os.getenv("SUPABASE_DB_PORT", "5432")
        sup_name = os.getenv("SUPABASE_DB_NAME")
        sup_user = os.getenv("SUPABASE_DB_USER")
        sup_password = os.getenv("SUPABASE_DB_PASSWORD")
        sup_sslmode = os.getenv("SUPABASE_SSLMODE", "require")

        missing = [k for k, v in (
            ("SUPABASE_DB_HOST", sup_host),
            ("SUPABASE_DB_NAME", sup_name),
            ("SUPABASE_DB_USER", sup_user),
            ("SUPABASE_DB_PASSWORD", sup_password),
        ) if not v]
        if missing:
            raise RuntimeError("Missing Supabase DB configuration: " + ", ".join(missing))
        dsn = _build_dsn_from_parts(sup_host, sup_port, sup_name, sup_user, sup_password, sup_sslmode)
        return psycopg2.connect(dsn)

    # 3) Generic DB_* env vars (local development)
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    sslmode = os.getenv("DB_SSLMODE", "require")

    missing = [k for k, v in (
        ("DB_HOST", host),
        ("DB_NAME", dbname),
        ("DB_USER", user),
        ("DB_PASSWORD", password),
    ) if not v]
    if missing:
        raise RuntimeError(
            "Missing DB configuration: " + ", ".join(missing)
            + ". Set DATABASE_URL, SUPABASE_DB_*, or DB_* env vars."
        )

    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password, sslmode=sslmode)
