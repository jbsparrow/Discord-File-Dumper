import asyncio
from collections.abc import AsyncGenerator
import aiofiles
import aiosqlite
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator





class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None

    async def async_init(self):
        self.conn = await aiosqlite.connect(self.db_path)

    async def close(self):
        await self.conn.close()

    async def get_users(self) -> AsyncGenerator:
        query = """
            SELECT id, name
            FROM users
        """
        async with self.conn.execute(query) as cursor:
            async for row in cursor:
                yield row

    async def get_media_by_user(self, user_id: str) -> AsyncGenerator:
        query = """
            SELECT *
            FROM media
            WHERE media.user_id = ?
        """
        async with self.conn.execute(query, (user_id,)) as cursor:
            async for row in cursor:
                yield row


class Dumper():
    def __init__(self):
        self.db = None

    async def async_init(self):
        self.db = Database("discord.db")

    async def run(self):
        await self.async_init()
        await self.db.async_init()
        await self.dump()
        await self.db.close()

    async def dump(self):
        async with aiofiles.open("output.txt", "w") as f:
            async for user in self.db.get_users():
                user_id = user[0]
                username = user[1]
                await f.write(f"=== {username} ({user_id})\n")
                async for row in self.db.get_media_by_user(user_id):
                    # file_id = row[0]
                    url = row[1]
                    # filename = row[2]
                    # size = row[3]
                    # content_type = row[4]
                    # width = row[5]
                    # height = row[6]
                    user_id = row[7]
                    # guild_id = row[8]
                    # channel_id = row[9]
                    # scrape_account_id = row[10]
                    # timestamp = row[11]
                    # search_timestamp = row[12]

                    await f.write(f"{url}\n")


async def main():
    dumper = Dumper()
    await dumper.run()


if __name__ == "__main__":
    asyncio.run(main())