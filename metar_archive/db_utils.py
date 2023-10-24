import asynch
import contextlib
import typing as tp
from asynch.pool import Pool
from asynch.cursors import DictCursor
from metar_archive.structures import _Base

pool: Pool | None = None


@contextlib.asynccontextmanager
async def with_clickhouse(**kwargs):
    global pool

    pool = await asynch.create_pool(**kwargs)
    yield pool

    pool.close()
    await pool.wait_closed()
    pool = None


@contextlib.asynccontextmanager
async def session_scope(cursor_type=DictCursor) -> DictCursor:
    if pool is None:
        raise RuntimeError("out of `with_clickhouse()` scope")

    async with pool.acquire() as conn:
        async with conn.cursor(cursor_type) as cursor:
            yield cursor


async def db_fetchall(
    model: tp.Type[_Base],
    query: str,
    query_args: dict[str, tp.Any] | None = None,
    name: str | None = None,
    *,
    raise_not_found: bool = True,
) -> tp.Any:
    if query_args is None:
        query_args = {}

    async with session_scope() as session:
        await session.execute(query, query_args)
        try:
            return model.from_rows(await session.fetchall())
        except AttributeError as exc:
            if raise_not_found:
                raise ValueError(name or model.__name__) from exc
            return []
