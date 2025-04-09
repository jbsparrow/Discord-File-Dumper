import requests
from typing import TYPE_CHECKING
import json
import aiohttp
from aiolimiter import AsyncLimiter
import asyncio
from aiohttp import ClientSession
from collections.abc import AsyncGenerator
from yarl import URL
import aiosqlite

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


class discordScraper():
    async def __init__(self, token):
        self.token = token
        self.main_url = "https://discord.com/api"
        self.headers = {
            "Authorization": token,
            "Content-Type": "application/json"
        }
        self.guilds = []
        self.guilds_media = []
        self.guilds_media_urls = []
        self.session = ClientSession()
        self.request_limiter = AsyncLimiter(1, 10)

    async def get_guilds(self):
        pass

    async def search_media(self, guild, timestamp: str = None) -> AsyncGenerator[dict]:
        request_json = {
            "tabs": {
                "media": {
                    "sort_by": "timestamp",
                    "sort_order": "desc",
                    "has": [
                        "image",
                        "video"
                    ],
                    "cursor": {
                        "timestamp": timestamp,
                        "type": "timestamp"
                    } if timestamp else None,
                    "limit": 25,
                }
            },
            "track_exact_total_hits": True,
        }
        request_url = self.main_url + "v9/guilds" / guild + "messages/search/tabs"

        while True:
            response = await self.session.post(request_url, headers=self.headers, json=request_json)
            data = await response.json()["tabs"]["media"]
            messages = data["tabs"]["media"].get("messages", [])

            if len(messages) > 0:
                yield messages
            else:
                break

            timestamp = data["cursor"]["timestamp"]
            request_json["tabs"]["media"]["cursor"] = {
                "timestamp": timestamp,
                "type": "timestamp"
            } if timestamp else None


    async def process_messages(self, guild):
        
        
        
        async for messages in self.search_media(guild):
            for message in messages:
                if "attachments" in message:
                    for attachment in message["attachments"]:
                        if attachment["url"] not in self.guilds_media_urls:
                            self.guilds_media_urls.append(attachment["url"])
                            self.guilds_media.append(attachment)


class database():
    async def __init__(self, db_path):
        self.connection = await aiosqlite.connect(db_path)
        self.cursor = await self.connection.cursor()
        await self.create_tables()

    async def create_tables(self):
        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS guilds (
                id INTEGER PRIMARY KEY,
                name TEXT,
                TIMESTAMP DEFAULT NULL
            )
            """
        )
        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
            """
        )
        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY,
                name TEXT,
                guild_id INTEGER,
                FOREIGN KEY (guild_id) REFERENCES guilds(id)
            )
            """
        )
        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS media (
                file_id INTEGER PRIMARY KEY,
                url TEXT,
                filename TEXT,
                size INTEGER,
                content_type TEXT,
                width INTEGER,
                height INTEGER,
                user_id INTEGER,
                guild_id INTEGER,
                channel_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (guild_id) REFERENCES guilds(id)
                FOREIGN KEY (channel_id) REFERENCES channels(id)
            )
            """
        )
        await self.connection.commit()


# Run the scraper
async def main():
    loop = asyncio.get_event_loop()
    database_instance = database("discord.db")

if __name__ == "__main__":
    asyncio.run(main())