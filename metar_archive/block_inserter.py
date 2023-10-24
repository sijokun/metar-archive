import datetime

from metar_archive.db_utils import session_scope, with_clickhouse
import asyncio
import os


class BlockInserter:
    def __init__(self, table_name: str, auto_write: int = 1000):
        self.buffer = []
        self.table_name = table_name
        self.auto_write = auto_write

    async def flush(self):
        tmp_buf = self.buffer
        self.buffer = []
        async with session_scope() as session:
            await session.execute(f"INSERT INTO {self.table_name} VALUES", tmp_buf)

    async def write(self, obj):
        self.buffer.append(obj)
        if len(self.buffer) >= self.auto_write:
            await self.flush()  # todo: task
