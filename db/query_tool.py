"""
Safe query helpers used by the FastAPI app.

Fixed:
- syntax error (unterminated string) removed
- imported get_connection so DB calls work
- added type hints and small defensive checks
- preserved existing behavior: validate SELECT-only, restrict schemas,
  provide available table suggestions when a table is missing.
"""
from typing import Any, Dict, List
import psycopg2
from psycopg2 import errors
from db.connection import get_connection


def _list_tables(schema: str) -> List[Dict[str, str]]:
    """Helper to list available tables for a given schema."""
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
        ORDER BY table_name;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (schema,))
            return [{"table_name": r[0]} for r in cur.fetchall()]


def query_company_db(sql_query: str) -> List[Dict[str, Any]]:
    """
    Executes a safe SELECT query on the 'company' schema only.
    Automatically provides suggestions if the target table doesn't exist.
    """
    if not isinstance(sql_query, str):
        raise ValueError("SQL query must be a string.")

    sql_clean = sql_query.strip().rstrip(";")
    sql_upper = sql_clean.upper()
    if not sql_upper.startswith("SELECT"):
        raise ValueError("Only SELECT statements are allowed.")
    if "company." not in sql_clean.lower():
        raise ValueError("Only queries on the 'company' schema are permitted.")

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_clean)
                columns = [desc[0] for desc in cur.description] if cur.description else []
                rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return rows
    except errors.UndefinedTable:
        available = _list_tables("company")
        # Raise ValueError with a payload (caller can format/inspect). This retains the
        # previous intent of providing available tables to the caller.
        raise ValueError({
            "error": "The specified table does not exist in the 'company' schema.",
            "available_tables": [t["table_name"] for t in available],
        })
    except Exception as e:
        # Wrap DB/other errors in ValueError to keep error type consistent for the app.
        raise ValueError(f"Database error: {str(e)}")


def query_admin_db(sql_query: str) -> List[Dict[str, Any]]:
    """
    Executes a safe SELECT query for admins.
    Admins may query 'company' and 'finance' schemas only.
    Automatically lists available tables if the target is missing.
    """
    if not isinstance(sql_query, str):
        raise ValueError("SQL query must be a string.")

    sql_clean = sql_query.strip().rstrip(";")
    sql_upper = sql_clean.upper()
    if not sql_upper.startswith("SELECT"):
        raise ValueError("Only SELECT statements are allowed.")

    lower_sql = sql_clean.lower()
    allowed_schemas: List[str] = []
    if "company." in lower_sql:
        allowed_schemas.append("company")
    if "finance." in lower_sql:
        allowed_schemas.append("finance")
    if not allowed_schemas:
        raise ValueError("Admins may only query 'company' or 'finance' schemas.")

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_clean)
                columns = [desc[0] for desc in cur.description] if cur.description else []
                rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return rows
    except errors.UndefinedTable:
        # Determine which schema(s) the query was targeting
        fallback_schemas = allowed_schemas or ["company", "finance"]
        available = {s: [t["table_name"] for t in _list_tables(s)] for s in fallback_schemas}
        raise ValueError({
            "error": "The specified table does not exist in one of the allowed schemas.",
            "available_tables": available,
        })
    except Exception as e:
        raise ValueError(f"Database error: {str(e)}")
