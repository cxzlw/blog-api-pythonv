import rfc3986
import aiomysql
import ipaddress

from urllib.parse import urlparse
from fastapi import FastAPI, Request
from pydantic import BaseModel, HttpUrl
from aiomysql.pool import Pool
from aiomysql.connection import Connection
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST"],
)

cf_ips = []
with open("cf_ips.txt") as f:
    for cidr in f.read().split("\n"):
        cf_ips.append(ipaddress.ip_network(cidr))


class CountRequest(BaseModel):
    page_url: HttpUrl


class CountResponse(BaseModel):
    page_pv: int
    page_uv: int
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


def normalize_url(url: str) -> str:
    url = url.removesuffix("index.html")
    url = rfc3986.normalize_uri(url)

    # Remove protocol, query and hash.
    parsed = rfc3986.urlparse(url)
    netloc = parsed.netloc
    path = parsed.path or ""
    url = netloc + path

    # Cloudflare normalize
    url = '/'.join(x for x in url.split('/') if x)

    return url


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/count")
async def page_count(count_request: CountRequest, request: Request):
    page_url = normalize_url(str(count_request.page_url))
    site = urlparse(page_url).netloc
    ip = get_ip_from_request(request)

    pool: Pool = app.db_pool

    async with pool.acquire() as conn:
        conn: Connection
        async with conn.cursor() as cur:
            await cur.execute("""
                    insert into access_record (url, site, ip_addr)
                    values (%s, %s, %s);
                """, (page_url, site, ip))
            await conn.commit()

            await cur.execute("""
                    select count(ip_addr), count(DISTINCT ip_addr)
                    from access_record
                    where url = %s; 
                """, (page_url,))
            page_pv, page_uv = await cur.fetchone()

            await cur.execute("""
                    select count(ip_addr), count(DISTINCT ip_addr)
                    from access_record
                    where site = %s; 
                """, (site,))
            site_pv, site_uv = await cur.fetchone()
    return CountResponse(page_pv=page_pv, page_uv=page_uv, site_pv=site_pv, site_uv=site_uv)


@app.on_event("startup")
async def startup():
    pool: Pool = await aiomysql.create_pool(
        host="127.0.0.1", user="blog_api_user",
        password="", db="blog_api",
    )
    app.db_pool = pool
    async with pool.acquire() as conn:
        conn: Connection
        async with conn.cursor() as cur:
            await cur.execute("""
                create table if not exists access_record
                (
                    id      int auto_increment primary key ,
                    url     varchar(1024), 
                    site     varchar(64), 
                    ip_addr varchar(64)
                );""")
            await conn.commit()


@app.on_event("shutdown")
async def shutdown():
    if hasattr(app, "db_pool"):
        app.db_pool.close()
