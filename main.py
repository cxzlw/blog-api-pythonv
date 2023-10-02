import asyncio
import rfc3986
import aiomysql
import ipaddress

from urllib.parse import urlparse
from fastapi import FastAPI, Request
from pydantic import BaseModel, HttpUrl
from aiomysql.connection import Connection

app = FastAPI()

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

    # Cloudflare normalize
    url = url.replace("\\", "/")
    url = '/'.join(x for x in url.split('/') if x)
    if url.startswith("http:/"):
        url = "http://" + url.removeprefix("http:/")
    if url.startswith("https:/"):
        url = "https://" + url.removeprefix("https:/")
    return url


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/count")
async def page_count(count_request: CountRequest, request: Request):
    page_url = normalize_url(str(count_request.page_url))
    site = urlparse(page_url).netloc
    ip = get_ip_from_request(request)

    conn: Connection = app.db_conn

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
    conn: Connection = await aiomysql.connect(
        host="127.0.0.1", user="blog_api_user",
        password="", db="blog_api",
        loop=asyncio.get_event_loop()
    )
    app.db_conn = conn
    async with conn.cursor() as cur:
        await cur.execute("""
            create table if not exists access_record
            (
                id      int auto_increment primary key ,
                url     varchar(1024), 
                site     varchar(64), 
                ip_addr varchar(15)
            );""")
        await conn.commit()


@app.on_event("shutdown")
async def shutdown():
    if hasattr(app, "db_conn"):
        app.db_conn.close()
