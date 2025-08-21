# app.py
from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from ilive_feed import get_digest, render_json, render_markdown, render_csv, FEED_URL

app = FastAPI(title="InvestingLive Digest API")

# Allow OpenAI Actions to call you (broad CORS is fine for server-to-server)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/digest")
def digest(
    format: str = Query("json", pattern="^(json|markdown|csv)$"),
    hours: Optional[int] = Query(None, ge=1, le=72),
    limit: int = Query(40, ge=1, le=200),
    url: str = Query(FEED_URL),
):
    items = get_digest(url=url, hours=hours, limit=limit)

    # If 304 / no change, return empty array or an informational message
    if format == "json":
        body = render_json(items)
        return Response(content=body, media_type="application/json; charset=utf-8")
    elif format == "markdown":
        body = render_markdown(items)
        return Response(content=body, media_type="text/markdown; charset=utf-8")
    else:
        body = render_csv(items)
        return Response(content=body, media_type="text/csv; charset=utf-8")
