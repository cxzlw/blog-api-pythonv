import rfc3986
import aiomysql
import ipaddress

from fastapi import FastAPI, Request
from pydantic import BaseModel
from aiomysql.pool import Pool
from aiomysql.connection import Connection
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
)

cf_ips = []
with open("cf_ips.txt") as f:
    for cidr in f.read().split("\n"):
        cf_ips.append(ipaddress.ip_network(cidr))


class CountResponse(BaseModel):
    page_pv: int
    page_uv: int
    page_mv: int
    site_pv: int
    site_uv: int


def is_cloudflare_ip(ip: str) -> bool:
    ip_addr = ipaddress.ip_address(ip)
    for cf_ip in cf_ips:
        if ip_addr in cf_ip:
            return True
    return False


def get_ip_from_request(request: Request) -> str:
    ip = request.client.host
    if is_cloudflare_ip(ip):
        return request.headers.get("CF-Connecting-IP") or ip
    return ip


def process_url(url: str) -> tuple[str, str]:
    url = url.removesuffix("index.html")
    url = rfc3986.normalize_uri(url)

    # Remove query and hash.
    parsed = rfc3986.urlparse(url)
    protocol = parsed.scheme
    netloc = parsed.netloc
    path = parsed.path or ""
    url = netloc + path

    # Cloudflare normalize
    url = "/".join(x for x in url.split("/") if x)

    return url, netloc


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/count")
async def get_page_count(page_url: str, request: Request) -> CountResponse:
    processed_page_url, site = process_url(page_url)
    user_ip = get_ip_from_request(request)
    pool: Pool = app.db_pool

    async with pool.acquire() as conn:
        conn: Connection
        async with conn.cursor() as cur:
            await cur.execute(
                """
                    select count(ip_addr), count(DISTINCT ip_addr)
                    from access_record
                    where url = %s; 
                """,
                (processed_page_url,),
            )
            page_pv, page_uv = await cur.fetchone()

            await cur.execute(
                """
                    select count(ip_addr)
                    from access_record
                    where url = %s and ip_addr = %s; 
                """,
                (processed_page_url, user_ip),
            )
            (page_mv,) = await cur.fetchone()

            await cur.execute(
                """
                    select count(ip_addr), count(DISTINCT ip_addr)
                    from access_record
                    where site = %s; 
                """,
                (site,),
            )
            site_pv, site_uv = await cur.fetchone()

    return CountResponse(
        page_pv=page_pv,
        page_uv=page_uv,
        page_mv=page_mv,
        site_pv=site_pv,
        site_uv=site_uv,
    )


@app.post("/count")
async def post_page_count(page_url: str, request: Request):
    processed_page_url, site = process_url(page_url)
    user_ip = get_ip_from_request(request)

    pool: Pool = app.db_pool

    async with pool.acquire() as conn:
        conn: Connection
        async with conn.cursor() as cur:
            await cur.execute(
                """
                    insert into access_record (url, site, ip_addr)
                    values (%s, %s, %s);
                """,
                (processed_page_url, site, user_ip),
            )
            await conn.commit()

    return await get_page_count(page_url, request)


@app.on_event("startup")
async def startup():
    pool: Pool = await aiomysql.create_pool(
        host="127.0.0.1",
        user="blog_api_user",
        password="",
        db="blog_api",
    )
    app.db_pool = pool
    async with pool.acquire() as conn:
        conn: Connection
        async with conn.cursor() as cur:
            await cur.execute(
                """
                create table if not exists access_record
                (
                    id      int auto_increment primary key ,
                    url     varchar(1024), 
                    site     varchar(64), 
                    ip_addr varchar(64)
                );"""
            )
            await conn.commit()


@app.on_event("shutdown")
async def shutdown():
    if hasattr(app, "db_pool"):
        app.db_pool.close()
