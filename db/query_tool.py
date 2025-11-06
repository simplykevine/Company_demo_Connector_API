# from db.connection import get_connection

# def query_company_db(sql: str) -> list[dict]:
#     sql_upper = sql.strip().upper()

#     if not sql_upper.startswith("SELECT"):
#         raise ValueError("Only SELECT statements are allowed.")
#     if "company." not in sql.lower():
#         raise ValueError("Only queries on the 'company' schema are permitted.")

#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             cur.execute(sql)
#             columns = [desc[0] for desc in cur.description]
#             rows = [dict(zip(columns, row)) for row in cur.fetchall()]
#     return rows

# # def query_admin_db(sql: str) -> list[dict]:
# #     """
# #     Allows an admin user to run SELECT queries
# #     on both the 'company' and 'finance' schemas.
# #     """

# #     sql_upper = sql.strip().upper()

# #     if not sql_upper.startswith("SELECT"):
# #         raise ValueError("Only SELECT statements are allowed.")

# #     lower_sql = sql.lower()
# #     if not ("company." in lower_sql or "finance." in lower_sql):
# #         raise ValueError("Admins may only query 'company' or 'finance' schemas.")
        
# #     with get_connection() as conn:
# #         with conn.cursor() as cur:
# #             cur.execute(sql)
# #             columns = [desc[0] for desc in cur.description]
# #             rows = [dict(zip(columns, row)) for row in cur.fetchall()]
# #     return rows

# import logging
# from psycopg2 import ProgrammingError
# logger = logging.getLogger(__name__)
# def query_admin_db(sql: str) -> list[dict]:
#     """
#     Allows an admin user to run safe SELECT queries across all schemas.
#     Enforces SELECT-only for security and logs all queries.
#     """
#     # Ensure query starts with SELECT
#     sql_upper = sql.strip().upper()
#     if not sql_upper.startswith("SELECT"):
#         raise ValueError("Only SELECT statements are allowed.")
#     logger.info(f"[ADMIN QUERY] Executing SQL: {sql}")
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             # Optional: expand schema visibility
#             cur.execute("SET search_path TO public, company, finance, actions;")
#             cur.execute(sql)
#             try:
#                 columns = [desc[0] for desc in cur.description]
#                 rows = [dict(zip(columns, row)) for row in cur.fetchall()]
#             except ProgrammingError:
#                 # Handles queries that don't return rows
#                 rows = []
#     return rows

from db.connection import get_connection
from psycopg2 import ProgrammingError
import re
def query_company_db(sql: str) -> list[dict]:
    sql_stripped = sql.strip()
    sql_upper = sql_stripped.upper()
    if not sql_upper.startswith("SELECT"):
        raise ValueError("Only SELECT statements are allowed.")
    lower_sql = sql_stripped.lower()
    if "company." not in lower_sql:
        raise ValueError("Only queries on the 'company' schema are permitted.")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SET search_path TO company;")
            cur.execute(sql_stripped)
            try:
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            except ProgrammingError:
                rows = []
    return rows
def resolve_table_schema(table_name: str, conn) -> str | None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_schema
            FROM information_schema.tables
            WHERE table_name = %s
              AND table_schema IN ('company', 'finance');
        """, (table_name,))
        row = cur.fetchone()
        return row[0] if row else None
def query_admin_db(sql: str) -> list[dict]:
    sql_stripped = sql.strip()
    sql_upper = sql_stripped.upper()
    if not sql_upper.startswith("SELECT"):
        raise ValueError("Only SELECT statements are allowed.")
    with get_connection() as conn:
        with conn.cursor() as cur:
            sql_lower = sql_stripped.lower()
            tables = re.findall(r"(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql_lower)
            for table in tables:
                if "." not in table:
                    schema = resolve_table_schema(table, conn)
                    if schema:
                        sql_stripped = re.sub(
                            rf"\b{table}\b", f"{schema}.{table}", sql_stripped, flags=re.IGNORECASE
                        )
                    else:
                        raise ValueError(
                            f"Table '{table}' not found in 'company' or 'finance' schemas."
                        )
            cur.execute("SET search_path TO company, finance;")
            cur.execute(sql_stripped)
            try:
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            except ProgrammingError:
                rows = []
    return rows
















