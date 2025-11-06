from fastapi import FastAPI, Request, HTTPException
from db.query_tool import query_company_db, query_admin_db
import os
from dotenv import load_dotenv
import uvicorn

load_dotenv()
app = FastAPI(title="Supabase Company API")

ADMIN_KEY = os.getenv("ADMIN_API_KEY")
USER_KEY = os.getenv("USER_API_KEY")


def check_auth(request: Request, required_key: str, role_name: str):
    auth_header = request.headers.get("Authorization")
    if auth_header != f"Bearer {required_key}":
        raise HTTPException(status_code=403, detail=f"Invalid {role_name} API key")
    return role_name


@app.post("/user/query")
async def user_query(request: Request):
    role = check_auth(request, USER_KEY, "user")
    data = await request.json()
    sql = data.get("sql")
    if not sql:
        raise HTTPException(status_code=400, detail="Missing SQL statement")

    try:
        results = query_company_db(sql)
        return {"status": "success", "rows": len(results), "results": results, "role": role}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/admin/query")
async def admin_query(request: Request):
    role = check_auth(request, ADMIN_KEY, "admin")
    data = await request.json()
    sql = data.get("sql")
    if not sql:
        raise HTTPException(status_code=400, detail="Missing SQL statement")

    try:
        results = query_admin_db(sql)
        return {"status": "success", "rows": len(results), "results": results, "role": role}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080)
