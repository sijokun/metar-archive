import datetime
import typing as tp

import pydantic


def model_to_tuple(pydantic_obj: pydantic.BaseModel) -> tuple[tp.Any, ...]:
    return tuple(pydantic_obj.model_dump().values())


class _Base(pydantic.BaseModel):
    @classmethod
    def from_row(cls, row: dict[tp.Any, tp.Any]) -> tp.Self:
        return cls.model_validate(row)

    @classmethod
    def from_rows(cls, rows: list[dict[tp.Any, tp.Any]]) -> list[tp.Self]:
        return [cls.from_row(x) for x in rows]


class Log(_Base):
    created: datetime.datetime
    filename: str
    func_name: str
    levelno: int
    lineno: int
    message: str
    name: str


class MetarRawBase(_Base):
    date: datetime.datetime
    icao_code: str


class MetarRaw(MetarRawBase):
    value: str

