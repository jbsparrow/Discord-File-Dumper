import argparse
import asyncio
import logging
from collections.abc import AsyncGenerator

import aiofiles
import aiosqlite

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def log(message: str, level: int = logging.INFO):
    if level == logging.INFO:
        logger.info(message)
    elif level == logging.ERROR:
        logger.error(message)
    elif level == logging.WARNING:
        logger.warning(message)
    elif level == logging.DEBUG:
        logger.debug(message)


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

    async def get_media_by_user(
        self,
        user_id: str,
        guild_id: str | None = None,
        channel_id: str | None = None,
        content_type: str | None = None,
        is_dm: bool | None = None,
        is_nsfw: bool | None = None,
    ) -> AsyncGenerator:
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

        async with self.conn.execute(query, tuple(params)) as cursor:
            async for row in cursor:
                yield row


class Dumper:
    def __init__(self, db_path: str, output_file: str, **filters):
        self.db = Database(db_path)
        self.output_file = output_file
        self.filters = filters

    async def run(self):
        await self.db.async_init()
        await self.dump()
        await self.db.close()

    async def dump(self):
        async with aiofiles.open(self.output_file, "w") as f:
            async for user in self.db.get_users():
                user_id = user[0]
                username = user[1]
                await f.write(f"=== {username} ({user_id})\n")

                async for row in self.db.get_media_by_user(user_id=user_id, **self.filters):
                    url = row[1]
                    await f.write(f"{url}\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Dump media URLs grouped by user with optional filters.")
    parser.add_argument("--db", default="discord.db", help="Path to the SQLite database.")
    parser.add_argument("--output", default="output.txt", help="Path to the output file.")
    parser.add_argument("--user-id", help="Filter by user ID.")
    parser.add_argument("--guild-id", help="Filter by guild ID.")
    parser.add_argument("--channel-id", help="Filter by channel ID.")
    parser.add_argument("--content-type", help="Filter by content type (e.g., image/png).")

    dm_group = parser.add_mutually_exclusive_group()
    dm_group.add_argument("--dm", dest="is_dm", action="store_const", const=True, help="Only include DMs.")
    dm_group.add_argument("--no-dm", dest="is_dm", action="store_const", const=False, help="Exclude DMs.")

    nsfw_group = parser.add_mutually_exclusive_group()
    nsfw_group.add_argument(
        "--nsfw", dest="is_nsfw", action="store_const", const=True, help="Only include NSFW content."
    )
    nsfw_group.add_argument(
        "--no-nsfw", dest="is_nsfw", action="store_const", const=False, help="Exclude NSFW content."
    )

    return parser.parse_args()


async def main():
    args = parse_args()

    filters = {
        "guild_id": args.guild_id,
        "channel_id": args.channel_id,
        "content_type": args.content_type,
        "is_dm": args.is_dm,
        "is_nsfw": args.is_nsfw,
    }

    dumper = Dumper(args.db, args.output, **filters)

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
