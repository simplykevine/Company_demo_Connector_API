import os
import psycopg2

def get_connection():
    # Prefer DATABASE_URL (Heroku); fall back to DB_* for local dev
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        sslmode = os.getenv("DB_SSLMODE", "require")
        return psycopg2.connect(database_url, sslmode=sslmode)

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
        raise RuntimeError("Missing DB configuration: " + ", ".join(missing) +
                           ". Set DATABASE_URL on Heroku or provide DB_* vars.")
    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password, sslmode=sslmode)
