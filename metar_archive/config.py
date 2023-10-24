from pprint import pprint

import pydantic_settings as pds


class _Config(pds.BaseSettings):
    model_config = pds.SettingsConfigDict(env_file='.env', case_sensitive=True, extra='forbid')
    CLICKHOUSE_HOST: str
    CLICKHOUSE_PORT: int = 9440
    CLICKHOUSE_PASSWORD: str


config = _Config()  # type: ignore

if __name__ == '__main__':
    pprint(vars(config))
