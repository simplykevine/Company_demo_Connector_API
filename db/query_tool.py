import psycopg2
from psycopg2 import errors
def _list_tables(schema: str) -> list[dict]:
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
def query_company_db(sql_query: str) -> list[dict]:
    """
    Executes a safe SELECT query on the 'company' schema only.
    Automatically provides suggestions if the target table doesn't exist.
    """
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
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return rows
    except errors.UndefinedTable:
        available = _list_tables("company")
        raise ValueError({
            "error": "The specified table does not exist in the 'company' schema.",
            "available_tables": [t["table_name"] for t in available],
        })
    except Exception as e:
        raise ValueError(f"Database error: {str(e)}")
def query_admin_db(sql_query: str) -> list[dict]:
    """
    Executes a safe SELECT query for admins.
    Admins may query 'company' and 'finance' schemas only.
    Automatically lists available tables if the target is missing.
    """
    sql_clean = sql_query.strip().rstrip(";")
    sql_upper = sql_clean.upper()
    if not sql_upper.startswith("SELECT"):
        raise ValueError("Only SELECT statements are allowed.")
    lower_sql = sql_clean.lower()
    allowed_schemas = []
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
                columns = [desc[0] for desc in cur.description]
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
        raise ValueError(f"Database error: {str(e)}")"
