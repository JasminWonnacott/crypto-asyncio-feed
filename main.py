
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import os
import asyncio
import aiohttp
import psycopg2


#  load environment variables
load_dotenv()


#  Connect to the database
conn = psycopg2.connect (
    dbname=os.getenv("db_name_sec"),
    user= os.getenv("user_sec"),
    password= os.getenv("postgres_key"),
    host="localhost"
)

#  create a cursor object
cur = conn.cursor()

async def get_api(session, params):
    base_url = "https://api.exchange.coinbase.com/products/BTC-USD/candles"

    async with session.get(base_url, params=params) as response:
        return await response.json()


def api_time_requests():
    end_time = datetime.now(tz=timezone.utc)
    start_time = end_time - timedelta(days=365)

    time_records = []
    while start_time < end_time:
        current_end = start_time + timedelta(hours=24)
        time_records.append((start_time, current_end))
        start_time = current_end

    return time_records


async def main():
    async with aiohttp.ClientSession() as session:
         time_records = api_time_requests()

         for start, end in time_records:
             start_iso = start.isoformat() + "Z"
             end_iso = end.isoformat() + "Z"

             params = {
                   "granularity" : "300",
                   "start" : start_iso,
                   "end" : end_iso
             }

             data = await get_api(session, params)


             for candle in data:
                 timestamp = datetime.fromtimestamp(candle[0], tz=timezone.utc)
                 cur.execute(
                    """
                    INSERT INTO coinbase_data (time, low, high, open, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (timestamp, candle[1], candle[2], candle[3], candle[4], candle[5])
                )

    conn.commit()
    cur.close()
    conn.close()




asyncio.run(main())







