import argparse
import asyncio
import logging
from argparse import BooleanOptionalAction
from collections.abc import AsyncGenerator
from datetime import datetime

import aiofiles
import aiosqlite
from yarl import URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def log(message: str, level: int = logging.INFO):
    """Log a message with the specified logging level."""
    logger.log(level, message)


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None

    async def async_init(self):
        self.conn = await aiosqlite.connect(self.db_path)

    async def close(self):
        await self.conn.close()

    async def get_users(self) -> AsyncGenerator:
        query = "SELECT id, name FROM users"
        async with self.conn.execute(query) as cursor:
            async for row in cursor:
                yield row

    async def generate_media_query(
        self,
        user_id: str,
        guild_id: str | None = None,
        channel_id: str | None = None,
        content_type: str | None = None,
        is_dm: bool | None = None,
        is_nsfw: bool | None = None,
    ):
        query = """
            SELECT media.* FROM media
            JOIN channels ON media.channel_id = channels.id
            WHERE media.user_id = ?
        """
        params = [user_id]

        if guild_id:
            query += " AND media.guild_id = ?"
            params.append(guild_id)
        if channel_id:
            query += " AND media.channel_id = ?"
            params.append(channel_id)
        if content_type:
            query += " AND media.content_type = ?"
            params.append(content_type)
        if is_dm is not None:
            query += " AND channels.is_dm = ?"
            params.append(int(is_dm))
        if is_nsfw is not None:
            query += " AND channels.is_nsfw = ?"
            params.append(int(is_nsfw))

        return query, tuple(params)

    async def check_user_has_media(self, query: str, params: tuple) -> bool:
        async with self.conn.execute(query, params) as cursor:
            row = await cursor.fetchone()
            return row is not None

    async def get_media_by_user(self, query: str, params: tuple) -> AsyncGenerator:
        async with self.conn.execute(query, params) as cursor:
            async for row in cursor:
                yield row


class Dumper:
    def __init__(self, args: dict, **filters):
        self.args = args
        self.db = Database(args.db)
        self.output_file = args.output
        self.filters = filters

    async def async_init_db(self):
        await self.db.async_init()

    async def run(self):
        await self.dump()
        await self.db.close()

    async def check_cdn_expired(self, url: URL | str) -> bool:
        if self.args.fix_cdn:
            url = URL(url)
            expiry = int(url.query.get("ex", 0), 16) * 1000
            if expiry <= datetime.now().timestamp() * 1000:
                return str(url.with_host("fixcdn.hyonsu.com"))
        return str(url)

    async def dump(self):
        async with aiofiles.open(self.output_file, "w") as f:
            async for user in self.db.get_users():
                user_id = user[0]
                query = await self.db.generate_media_query(user_id=user_id, **self.filters)
                has_media = await self.db.check_user_has_media(query[0], query[1])
                if has_media:
                    username = user[1]
                    await f.write(f"=== {username} ({user_id})\n")

                    async for row in self.db.get_media_by_user(query[0], query[1]):
                        url = await self.check_cdn_expired(row[1])
                        await f.write(f"{url}\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Dump media URLs grouped by user with optional filters.")
    parser.add_argument("--db", default="discord.db", help="Path to the SQLite database. (default: discord.db)")
    parser.add_argument("--output", default="output.txt", help="Path to the output file. (default: output.txt)")
    parser.add_argument("--user-id", help="Filter by user ID.")
    parser.add_argument("--guild-id", help="Filter by guild ID.")
    parser.add_argument("--channel-id", help="Filter by channel ID.")
    parser.add_argument("--content-type", help="Filter by content type (e.g., image/png).")
    parser.add_argument(
        "--fix-cdn", action=BooleanOptionalAction, default=True, help="Fix expired CDN URLs. (default: True)"
    )

    parser.add_argument("--dm", action=BooleanOptionalAction, default=None, help="Only include DMs.")
    parser.add_argument("--nsfw", action=BooleanOptionalAction, default=None, help="Only include NSFW content.")

    return parser.parse_args()


async def main():
    args = parse_args()
    if args.dm and args.nsfw:
        log("Cannot use --dm and --nsfw together.", logging.ERROR)
        return

    filters = {
        "guild_id": args.guild_id,
        "channel_id": args.channel_id,
        "content_type": args.content_type,
        "is_dm": args.dm,
        "is_nsfw": args.nsfw,
    }

    dumper = Dumper(args, **filters)
    await dumper.async_init_db()

    if args.user_id:
        query = "SELECT name FROM users WHERE id = ?"
        async with dumper.db.conn.execute(query, (args.user_id,)) as cursor:
            row = await cursor.fetchone()
            if row is None:
                log(f"User ID {args.user_id} not found in the database.", logging.ERROR)
                return
            username = row[0]

        async def patched_get_users():
            yield (args.user_id, username)

        dumper.db.get_users = patched_get_users

    await dumper.run()


if __name__ == "__main__":
    asyncio.run(main())
