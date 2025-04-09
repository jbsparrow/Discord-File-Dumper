import json
import aiohttp
import asyncio
from aiolimiter import AsyncLimiter
from aiohttp import ClientSession
from collections.abc import AsyncGenerator
from yarl import URL
import aiosqlite
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


class DiscordScraper:
    def __init__(self, token):
        self.token = token
        self.main_url = URL("https://discord.com/api")
        self.headers = {
            "Authorization": token,
            "Content-Type": "application/json"
        }
        self.guilds = []
        self.guilds_media = []
        self.guilds_media_urls = []
        self.session = None
        self.db = Database("discord.db")
        self.request_limiter = AsyncLimiter(1, 10)

    async def async_init(self):
        self.session = ClientSession()
        await self.db.async_init()

    async def get_guilds(self) -> list[int]:
        api_endpoint = self.main_url / "v9/users" / "@me" / "guilds"

        async with self.session.get(api_endpoint, headers=self.headers) as response:
            if response.status == 200:
                guilds = await response.json()
                guilds = [guild["id"] for guild in guilds]
                for guild in guilds:
                    await self.db.insert_guild(guild, guild["name"])
            else:
                raise Exception(f"Failed to fetch guilds: {response.status}")

    async def search_guild_media(self, guild, timestamp: str = None) -> AsyncGenerator[dict, None]:
        request_json = {
            "tabs": {
                "media": {
                    "sort_by": "timestamp",
                    "sort_order": "desc",
                    "has": ["image", "video"],
                    "cursor": {
                        "timestamp": timestamp,
                        "type": "timestamp"
                    } if timestamp else None,
                    "limit": 25,
                }
            },
            "track_exact_total_hits": True,
        }

        request_url = self.main_url / "v9/guilds" / guild / "messages/search/tabs"

        while True:
            async with self.session.post(request_url, headers=self.headers, json=request_json) as response:
                data = await response.json()
                media = data.get("tabs", {}).get("media", {})
                messages = media.get("messages", [])

                if messages:
                    yield messages
                else:
                    break

                timestamp = media.get("cursor", {}).get("timestamp")
                if timestamp:
                    request_json["tabs"]["media"]["cursor"] = {
                        "timestamp": timestamp,
                        "type": "timestamp"
                    }

    async def search_direct_message_media(self) -> AsyncGenerator[dict, None]:
        request_json = {
            "tabs": {
                "media": {
                    "sort_by": "timestamp",
                    "sort_order": "desc",
                    "has": ["image", "video"],
                    "limit": 25,
                }
            },
            "track_exact_total_hits": True,
        }

        request_url = self.main_url / "v9/users" / "@me" / "messages/search/tabs"

        while True:
            async with self.session.post(request_url, headers=self.headers, json=request_json) as response:
                data = await response.json()
                media = data.get("tabs", {}).get("media", {})
                messages = media.get("messages", [])

                if messages:
                    yield messages
                else:
                    break

    async def process_messages(self, guild):
        async for messages in self.search_media(guild):
            for message in messages:
                for attachment in message.get("attachments", []):
                    url = attachment.get("url")
                    if url and url not in self.guilds_media_urls:
                        self.guilds_media_urls.append(url)
                        self.guilds_media.append(attachment)

    async def close(self):
        if self.session:
            await self.session.close()


class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        self.connection = None
        self.cursor = None

    async def async_init(self):
        self.connection = await aiosqlite.connect(self.db_path)
        self.cursor = await self.connection.cursor()
        await self.create_tables()

    async def create_tables(self):
        await self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS guilds (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """)
        await self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """)
        await self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY,
                name TEXT,
                guild_id INTEGER,
                FOREIGN KEY (guild_id) REFERENCES guilds(id)
            )
        """)
        await self.cursor.execute("""
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
                account_id INTEGER,
                timestamp INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (guild_id) REFERENCES guilds(id),
                FOREIGN KEY (channel_id) REFERENCES channels(id),
                FOREIGN KEY (account_id) REFERENCES main_users(id)
            )
        """)
        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS main_users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                FOREIGN KEY (id) REFERENCES users(id)
            )
            """)
        await self.connection.commit()

    async def insert_guild(self, guild_id, name):
        await self.cursor.execute("INSERT OR IGNORE INTO guilds (id, name) VALUES (?, ?)", (guild_id, name))
        await self.connection.commit()
        await self.cursor.execute("UPDATE guilds SET name = ? WHERE id = ?", (name, guild_id))
        await self.connection.commit()

    async def insert_user(self, user_id, name):
        await self.cursor.execute("INSERT OR IGNORE INTO users (id, name) VALUES (?, ?)", (user_id, name))
        await self.connection.commit()
        await self.cursor.execute("UPDATE users SET name = ? WHERE id = ?", (name, user_id))
        await self.connection.commit()

    async def insert_channel(self, channel_id, name, guild_id):
        await self.cursor.execute("INSERT OR IGNORE INTO channels (id, name, guild_id) VALUES (?, ?, ?)",
                                (channel_id, name, guild_id))
        await self.connection.commit()
        await self.cursor.execute("UPDATE channels SET name = ? WHERE id = ?", (name, channel_id))
        await self.connection.commit()

    async def insert_main_user(self, user_id, username):
        await self.cursor.execute("INSERT OR IGNORE INTO main_users (id, name) VALUES (?, ?)", (user_id, username))
        await self.connection.commit()
        await self.cursor.execute("UPDATE main_users SET name = ? WHERE id = ?", (username, user_id))
        await self.connection.commit()

    async def insert_user(self, user_id, username):
        await self.cursor.execute("INSERT OR IGNORE INTO users (id, name) VALUES (?, ?)", (user_id, username))
        await self.connection.commit()
        await self.cursor.execute("UPDATE users SET name = ? WHERE id = ?", (username, user_id))
        await self.connection.commit()

    async def close(self):
        if self.connection:
            await self.connection.close()


# Run the scraper
async def main():
    db = Database("discord.db")
    await db.async_init()

    # scraper = DiscordScraper("your_token_here")
    # await scraper.async_init()

    # You can call scraper methods here, e.g.:
    # await scraper.process_messages(guild_id)

    # await scraper.close()
    await db.close()


if __name__ == "__main__":
    asyncio.run(main())