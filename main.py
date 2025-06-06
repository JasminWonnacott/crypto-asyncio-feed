import json
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import os
import asyncio
import aiohttp
import asyncpg


#  load environment variables
load_dotenv()

async def get_api(session, params):
    base_url = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
    async with session.get(base_url, params=params) as response:
        if response.status !=200:
            print(f"Error bad response status {response.status}")
            return []
        try:
            data = await response.json()
            if not isinstance(data, list):
                print(f"Error data is not in a list")
                return []
            return data
        except json.JSONDecodeError:
            print(f"Error parsing to JSON")
            return []


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
    pool = await asyncpg.create_pool(
        database=os.getenv("db_name"),
        user=os.getenv("user_sec"),
        password=os.getenv("postgres_key"),
        host=os.getenv("local_host")
    )

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
                 async with pool.acquire() as conn:
                     await conn.execute(
                    """
                    INSERT INTO coinbase_data (time, low, high, open, close, volume)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    """,
                    timestamp, candle[1], candle[2], candle[3], candle[4], candle[5]
                )

    await pool.close()
asyncio.run(main())







