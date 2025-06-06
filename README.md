# Coinbase BTC-USD Candle Scraper

This project collects 365 days worth of historical 5-minute candlestick data for Bitcoin from Coinbase's public API and stores it in a PostgreSQL database. It's built using Python, asyncio, aiohttp, and asyncpg.

Features

- Pulls 365 days of 5-minute BTC-USD candlestick data
- Asynchronous API requests for fast performance
- Stores data in PostgreSQL using asyncpg
- Parses and handles JSON responses safely
- Modular and ready to expand with more indicators

Technologies Used

- Python 3.10+
- aiohttp
- asyncpg
- dotenv
- PostgreSQL

