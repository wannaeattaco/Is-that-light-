import pymysql
import uvicorn
from typing import Optional
from fastapi import FastAPI
from pydantic import BaseModel
from dbutils.pooled_db import PooledDB
from config import DB_HOST, DB_USER, DB_PASSWD, DB_NAME


app = FastAPI()
pool = PooledDB(creator=pymysql,
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWD,
                database=DB_NAME,
                maxconnections=1,
                blocking=True)


class Data(BaseModel):
    id: Optional[int]
    ts: Optional[int]
    lat: Optional[int]
    lon: Optional[int]
    temp: Optional[int]
    light: Optional[int]
    pm25: Optional[int]
    humidity: Optional[int]


@app.get("/com_indoor/")
async def get_com_indoor():
    with pool.connection() as conn, conn.cursor() as cs:
        cs.execute("""
                SELECT id, ts, lat, lon, temp, light 
                FROM com_indoor 
                        """)
        result = [Data(*row) for row in cs.fetchall()]
        return result


@app.get("/eng_lib")
async def get_com_indoor():
    with pool.connection() as conn, conn.cursor() as cs:
        cs.execute("""
                SELECT id, ts, lat, lon, temp, light 
                FROM eng_lib 
                        """)
        result = [Data(*row) for row in cs.fetchall()]
        return result


@app.get("/main_ku")
async def get_com_indoor():
    with pool.connection() as conn, conn.cursor() as cs:
        cs.execute("""
                SELECT id, ts, lat, lon, temp, light 
                FROM main_ku 
                        """)
        result = [Data(*row) for row in cs.fetchall()]
        return result


@app.get("/Econ_lib")
async def get_com_indoor():
    with pool.connection() as conn, conn.cursor() as cs:
        cs.execute("""
                SELECT id, ts, lat, lon, temp, light 
                FROM Econ_lib
                        """)
        result = [Data(*row) for row in cs.fetchall()]
        return result


@app.get("/com_outdoor")
async def get_com_indoor():
    with pool.connection() as conn, conn.cursor() as cs:
        cs.execute("""
        SELECT id, ts, lat, lon, temp, light, pm25, humidity 
        FROM com_outdoor
        """)
        result = [Data(*row) for row in cs.fetchall()]
        return result


@app.get("/Econ_outdoor")
async def get_com_indoor():
    with pool.connection() as conn, conn.cursor() as cs:
        cs.execute("""
        SELECT id, ts, lat, lon, temp, light, pm25, humidity 
        FROM Econ_outdoor
        """)
        result = [Data(*row) for row in cs.fetchall()]
        return result


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000)
