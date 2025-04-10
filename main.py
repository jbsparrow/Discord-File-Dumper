import argparse
import asyncio
import logging
from collections.abc import AsyncGenerator

import aiosqlite
import dotenv
from aiohttp import ClientSession
from aiolimiter import AsyncLimiter
from yarl import URL

# Argparse setup
parser = argparse.ArgumentParser(description="Discord Media Scraper")
parser.add_argument("--deep-scrape", action="store_true", help="Perform a deep scrape of all channels and messages")
args = parser.parse_args()


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def log(message: str, level: int = logging.INFO):
    """Log a message with the specified logging level."""
    logger.log(level, message)


class DiscordScraper:
    def __init__(self, token, user_id: str | None, username: str | None):
        self.token = token
        self.user_id = user_id
        self.username = username
        self.main_url = URL("https://discord.com/api")
        self.start_count = 0
        self.headers = {"Authorization": token, "Content-Type": "application/json"}
        self.session = None
        self.db = Database("discord.db")
        self.request_limiter = AsyncLimiter(5, 2)

    async def async_init(self):
        self.session = ClientSession()
        await self.db.async_init()
        await self.db.insert_scraping_account(self.user_id, self.username)
        await self.db.insert_guild("@me", "DMs")
        self.start_count = await self.db.count_media()

    async def get_guilds(self) -> None:
        api_endpoint = self.main_url / "v9/users" / "@me" / "guilds"

        async with self.request_limiter:
            async with self.session.get(api_endpoint, headers=self.headers) as response:
                if response.status == 200:
                    guilds = await response.json()
                    for guild in guilds:
                        await self.db.insert_guild(guild.get("id"), guild.get("name"))
                        log(f"Found guild: {guild.get('id')} {guild.get('name')}", logging.INFO)
                else:
                    raise Exception(f"Failed to fetch guilds: {response.status}")

    async def get_guild_channels(self, guild_id: str | None, guild_name: str | None) -> None:
        if guild_id:
            guilds = [(guild_id, f"Retrying {guild_name}")]
        else:
            guilds = await self.db.get_guilds()
        async with AsyncLimiter(10):
            for guild in guilds:
                await asyncio.sleep(0.5)
                guild_id = guild[0]
                guild_name = guild[1]
                log(f"Getting channels for guild: {guild_id} {guild_name}", logging.INFO)
                api_endpoint = self.main_url / "v9" / "guilds" / guild_id / "channels"

                async with self.session.get(api_endpoint, headers=self.headers) as response:
                    if response.status == 200:
                        channels = await response.json()
                        for channel in channels:
                            if channel.get("type", -1) == 0:  # Text channel
                                channel_id = channel.get("id", 0)
                                channel_name = channel.get("name", "")
                                is_nsfw = channel.get("nsfw", False)
                                await self.db.insert_channel(channel_id, channel_name, guild_id, is_nsfw, False)
                    else:
                        if response.status == 429:
                            log("Rate limited, retrying in 5s...", logging.WARNING)
                            await asyncio.sleep(5)
                            await self.get_guild_channels(guild_id, guild_name)
                        elif response.status == 403:
                            log(f"Forbidden access to guild: {guild_id} {guild_name}", logging.WARNING)
                            await self.db.remove_guild(guild_id)
                        elif response.status == 404:
                            log(f"Guild not found: {guild_id} {guild_name}", logging.WARNING)
                            await self.db.remove_guild(guild_id)
                        else:
                            raise Exception(f"Failed to fetch channels for guild {guild_id}: {response.status}")

    async def search_guild_media(self, guild, timestamp: str | None) -> AsyncGenerator[dict, None]:
        log(f"Searching media in guild: {guild}", logging.INFO)
        request_json = {
            "include_nsfw": True,
            "tabs": {
                "media": {
                    "sort_by": "timestamp",
                    "sort_order": "asc",
                    "has": ["image", "video"],
                    "cursor": {"timestamp": timestamp, "type": "timestamp"} if timestamp else None,
                    "limit": 25,
                }
            },
            "track_exact_total_hits": True,
        }

        request_url = self.main_url / "v9/guilds" / guild / "messages/search/tabs"

        while True:
            async with self.request_limiter:
                async with self.session.post(request_url, headers=self.headers, json=request_json) as response:
                    data = await response.json()
                    if "rate limited" in data.get("message", ""):
                        sleep_time = data.get("retry_after", 0)
                        await asyncio.sleep(sleep_time * 1.2)
                        continue
                    media = data.get("tabs", {}).get("media", {})
                    messages = media.get("messages", [])

                    if messages:
                        timestamp = media.get("cursor", {}).get("timestamp")
                        yield messages, timestamp
                    else:
                        break

                    if timestamp:
                        request_json["tabs"]["media"]["cursor"] = {"timestamp": timestamp, "type": "timestamp"}

    async def search_dm_media(self, timestamp: str | None) -> AsyncGenerator[dict, None]:
        request_json = {
            "include_nsfw": True,
            "tabs": {
                "media": {
                    "sort_by": "timestamp",
                    "sort_order": "asc",
                    "has": ["image", "video"],
                    "cursor": {"timestamp": timestamp, "type": "timestamp"} if timestamp else None,
                    "limit": 25,
                }
            },
            "track_exact_total_hits": True,
        }

        request_url = self.main_url / "v9/users" / "@me" / "messages/search/tabs"

        while True:
            async with self.request_limiter:
                async with self.session.post(request_url, headers=self.headers, json=request_json) as response:
                    data = await response.json()
                    if "rate limited" in data.get("message", ""):
                        sleep_time = data.get("retry_after", 0)
                        await asyncio.sleep(sleep_time * 1.2)
                        continue
                    media = data.get("tabs", {}).get("media", {})
                    messages = media.get("messages", [])

                    if messages:
                        timestamp = media.get("cursor", {}).get("timestamp")
                        yield messages, timestamp
                    else:
                        break

                    if timestamp:
                        request_json["tabs"]["media"]["cursor"] = {"timestamp": timestamp, "type": "timestamp"}

    async def process_guild_messages(self):
        guilds = await self.db.get_guilds()
        for guild in guilds:
            guild_id = guild[0]
            last_timestamp = guild[2] if not args.deep_scrape else None
            async for messages, search_timestamp in self.search_guild_media(guild_id, last_timestamp):
                for message in messages:
                    message = message[0]
                    await self.process_message(message, guild_id, search_timestamp)

    async def process_dms(self):
        guild = await self.db.get_guilds(get_dms=True)
        last_timestamp = guild[2] if not args.deep_scrape else None
        async for messages, search_timestamp in self.search_dm_media(last_timestamp):
            for message in messages:
                message = message[0]
                await self.process_message(message, "@me", search_timestamp)

    async def process_message(self, message, guild_id: str, search_timestamp: str):
        for attachment in message.get("attachments", []):
            file_id = attachment.get("id", 0)
            url = attachment.get("url")
            filename = attachment.get("filename")
            size = attachment.get("size", 0)
            content_type = attachment.get("content_type")
            width = attachment.get("width", 0)
            height = attachment.get("height", 0)
            user_id = message.get("author", {}).get("id")
            username = message.get("author", {}).get("username")
            channel_id = message.get("channel_id")
            timestamp = message.get("timestamp")
            if url:
                await self.db.insert_media(
                    file_id=file_id,
                    url=url,
                    filename=filename,
                    size=size,
                    content_type=content_type,
                    width=width,
                    height=height,
                    user_id=user_id,
                    guild_id=guild_id,
                    channel_id=channel_id,
                    account_id=self.user_id,
                    timestamp=timestamp,
                    search_timestamp=search_timestamp,
                )
                await self.db.insert_user(user_id, username, None)
                await self.db.update_guild_timestamp(guild_id, search_timestamp)
                if guild_id == "@me":
                    await self.db.insert_channel(channel_id, f"{username} DMs", guild_id, False, True)

    async def get_new_count(self):
        self.end_count = await self.db.count_media()
        self.new_count = self.end_count - self.start_count
        return self.new_count

    async def close(self):
        if self.session:
            await self.session.close()
        if self.db:
            await self.db.close()


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
            CREATE TABLE IF NOT EXISTS accounts (
                id TEXT PRIMARY KEY,
                name TEXT
            )
        """)

        await self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                name TEXT
            )
        """)

        await self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS guilds (
                id TEXT PRIMARY KEY,
                name TEXT,
                last_timestamp TEXT
            )
        """)

        await self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                id TEXT PRIMARY KEY,
                name TEXT,
                is_dm INTEGER,
                is_nsfw INTEGER,
                guild_id TEXT,
                FOREIGN KEY (guild_id) REFERENCES guilds(id)
            )
        """)

        await self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS media (
                file_id TEXT PRIMARY KEY,
                url TEXT,
                filename TEXT,
                size INTEGER,
                content_type TEXT,
                width INTEGER,
                height INTEGER,
                user_id TEXT,
                guild_id TEXT,
                channel_id TEXT,
                account_id TEXT,
                timestamp TEXT,
                search_timestamp TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (guild_id) REFERENCES guilds(id),
                FOREIGN KEY (channel_id) REFERENCES channels(id),
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            )
        """)

        await self.connection.commit()

    async def insert_guild(self, guild_id: str, name: str):
        await self.cursor.execute(
            """
            INSERT OR IGNORE INTO guilds (id, name) VALUES (?, ?)
            ON CONFLICT(id) DO UPDATE SET name = excluded.name
            """,
            (guild_id, name),
        )
        await self.connection.commit()

    async def insert_user(self, user_id: str, username: str, channel_id: str | None):
        await self.cursor.execute(
            """
            INSERT OR IGNORE INTO users (id, name) VALUES (?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET name = excluded.name
            """,
            (user_id, username),
        )
        await self.connection.commit()

    async def insert_channel(
        self, channel_id: str, name: str, guild_id: str, is_nsfw: bool = False, is_dm: bool = False
    ):
        await self.cursor.execute(
            """
            INSERT OR IGNORE INTO channels (id, name, is_dm, is_nsfw, guild_id) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET name = excluded.name, is_dm = excluded.is_dm, is_nsfw = excluded.is_nsfw
            """,
            (channel_id, name, is_dm, is_nsfw, guild_id),
        )
        await self.connection.commit()

    async def insert_scraping_account(self, user_id: str, username: str):
        await self.cursor.execute(
            """
            INSERT OR IGNORE INTO accounts (id, name) VALUES (?, ?)
            ON CONFLICT(id) DO UPDATE SET name = excluded.name
            """,
            (user_id, username),
        )
        await self.connection.commit()

    async def insert_media(
        self,
        file_id: str,
        url: str,
        filename: str,
        size: int,
        content_type: str,
        width: int,
        height: int,
        user_id: str,
        guild_id: str,
        channel_id: str,
        account_id: str,
        timestamp: str,
        search_timestamp: str,
    ):
        await self.cursor.execute(
            """
            INSERT INTO media (file_id, url, filename, size, content_type, width, height, user_id, guild_id,
                                        channel_id, account_id, timestamp, search_timestamp)
            VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(file_id) DO UPDATE SET url=excluded.url
        """,
            (
                file_id,
                url,
                filename,
                size,
                content_type,
                width,
                height,
                user_id,
                guild_id,
                channel_id,
                account_id,
                timestamp,
                search_timestamp,
            ),
        )
        await self.connection.commit()

    async def update_guild_timestamp(self, guild_id: str, timestamp: str):
        await self.cursor.execute("UPDATE guilds SET last_timestamp = ? WHERE id = ?", (timestamp, guild_id))
        await self.connection.commit()

    async def get_guilds(self, get_dms: bool = False) -> list[tuple[str, str]]:
        if get_dms:
            await self.cursor.execute("SELECT * FROM guilds WHERE id = '@me'")
            return await self.cursor.fetchone()
        await self.cursor.execute("SELECT * FROM guilds")
        guilds = await self.cursor.fetchall()
        return [guild for guild in guilds if guild[0] != "@me"]

    async def get_channels(self, guild_id: str | None, is_nsfw: bool = False):
        if guild_id:
            await self.cursor.execute("SELECT * FROM channels WHERE guild_id = ? AND is_nsfw = ?", (guild_id, is_nsfw))
        else:
            await self.cursor.execute("SELECT * FROM channels WHERE is_nsfw = ?", (is_nsfw,))
        return await self.cursor.fetchall()

    async def remove_guild(self, guild_id: str):
        await self.cursor.execute("DELETE FROM guilds WHERE id = ?", (guild_id,))
        await self.connection.commit()

    async def count_media(self):
        await self.cursor.execute("SELECT COUNT(*) FROM media")
        count = await self.cursor.fetchone()
        return count[0] if count else 0

    async def close(self):
        if self.connection:
            await self.cursor.close()
            await self.connection.close()


# Run the scraper
async def main():
    dotenv_path = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenv_path)
    token = str(dotenv.get_key(dotenv_path, "DISCORD_TOKEN"))
    user_id = str(dotenv.get_key(dotenv_path, "DISCORD_USER_ID"))
    username = str(dotenv.get_key(dotenv_path, "DISCORD_USERNAME"))

    scraper = DiscordScraper(token, user_id, username)
    await scraper.async_init()

    # You can call scraper methods here, e.g.:
    log("Getting Guilds...", logging.INFO)
    await scraper.get_guilds()
    log("Getting Guild Channels...", logging.INFO)
    await scraper.get_guild_channels(None, None)
    log("Processing Server Media...", logging.INFO)
    await scraper.process_guild_messages()
    log("Processing DM Media...", logging.INFO)
    await scraper.process_dms()
    log("Done!", logging.INFO)

    new_count = await scraper.get_new_count()
    total_count = await scraper.db.count_media()
    log(f"Found: {new_count} new media items.\nTotal: {total_count} media items.", logging.INFO)

    await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
