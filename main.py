from metar_archive.config import config
from metar import Metar
import requests
import typing as tp
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import dataclasses
import asyncio
import aiohttp
import traceback
import sys
from metar_archive.block_inserter import BlockInserter
from metar_archive.metar_parser import parse_metar

from aiolimiter import AsyncLimiter

from metar_archive.logging_utils import clickhouse_logger, logger

from metar_archive.db_utils import with_clickhouse, db_fetchall

from metar_archive import structures

inserter = BlockInserter("metars", auto_write=1000)
limiter = AsyncLimiter(200, 1)
cycle_limiter = AsyncLimiter(1, 60)


def get_traceback_string(exception):
    if hasattr(exception, "__traceback__"):
        tb_strings = traceback.format_tb(exception.__traceback__)
    else:
        tb_strings = traceback.format_exception(*sys.exc_info())
    return "".join(tb_strings)


def format_exception(e, with_traceback=False):
    if hasattr(e, "__module__"):
        exc_string = "{}.{}: {}".format(e.__module__, e.__class__.__name__, e)
    else:
        exc_string = "{}: {}".format(e.__class__.__name__, e)

    if with_traceback:
        traceback_string = ":\n" + get_traceback_string(exception=e)
    else:
        traceback_string = ""

    return "{}{}".format(exc_string, traceback_string)


@dataclasses.dataclass
class Metar:
    metar: str
    date: datetime

    def __repr__(self):
        return f"[{self.date}] {self.metar}"


@dataclasses.dataclass
class Station:
    code: str
    link: str
    last_update: datetime

    async def get_metar(self, retry: int = 0) -> Metar | None:
        try:
            async with limiter:
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.link) as response:
                        r = await response.text()

            date_string, metar = r.strip().split("\n")[:2]
            date_format = "%Y/%m/%d %H:%M"
            parsed_date = datetime.strptime(date_string, date_format)
            return Metar(metar, parsed_date)
        except Exception as e:
            if isinstance(e, UnicodeDecodeError) or isinstance(e, ValueError):
                return

            if retry < 5:
                return await self.get_metar(retry + 1)
            else:
                logger.error(self.link + " " + format_exception(e))
                return


def get_list_of_stations() -> list[Station]:
    base_url = "https://tgftp.nws.noaa.gov/data/observations/metar/stations/"
    r = requests.get(base_url)
    soup = BeautifulSoup(r.content, "html.parser")
    table = soup.find("table")
    rows = table.find_all("tr")

    stations = []
    for row in rows[3:-1]:
        columns = row.find_all("td")
        a_tag = columns[0].find("a")
        date_format = "%d-%b-%Y %H:%M"
        parsed_date = datetime.strptime(columns[1].text.strip(), date_format)

        stations.append(
            Station(
                code=a_tag.text.removesuffix(".TXT"),
                link=base_url + a_tag.attrs["href"],
                last_update=parsed_date,
            )
        )

    return stations


async def process_station(station, latest_dates):
    metar = await station.get_metar()
    if metar is None:
        return

    if latest_dates.get(station.code) != metar.date:
        parsed = parse_metar(metar.metar)
        parsed["date"] = metar.date
        parsed["icao_code"] = station.code

        await inserter.write(parsed)
    else:
        logger.debug(
            f"Skipping duplicate for {station.code}, {latest_dates[station.code]} == {metar.date}"
        )


async def update():
    logger.info('Starting update cycle')
    logger.info('Getting stations')
    stations = get_list_of_stations()
    logger.info('Getting latest records')
    latest_records: tp.List[structures.MetarRawBase] = await db_fetchall(
        structures.MetarRawBase,
        "SELECT icao_code, max(date) AS date FROM metars GROUP BY icao_code",
        raise_not_found=False,
    )

    latest_dates: tp.Dict[str, datetime] = dict()

    for record in latest_records:
        latest_dates[record.icao_code] = record.date

    logger.info('Filtering stations')
    stations_filtered = filter(
        lambda s: (
            s.last_update - latest_dates.get(s.code, datetime.fromtimestamp(0))
        )
        > timedelta(minutes=25),
        stations,
    )

    logger.info('Preparing tasks')
    tasks = [
        process_station(station, latest_dates) for station in stations_filtered
    ]

    logger.info(f"Starting {len(tasks)} tasks")
    await asyncio.gather(*tasks)
    logger.info(f"Flushing ends")
    await inserter.flush()
    logger.info(f"Update cycle finished")


async def main():
    async with with_clickhouse(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            database='default',
            user='default',
            password=config.CLICKHOUSE_PASSWORD,
            secure=True,
            echo=False,
            minsize=10,
            maxsize=50,
    ):
        async with clickhouse_logger():
            while True:
                async with cycle_limiter:
                    await update()

if __name__ == "__main__":
    asyncio.run(main())
