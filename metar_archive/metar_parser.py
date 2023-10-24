from metar import Metar
from metar_archive.logging_utils import logger


def parse_metar(raw: str):
    output = {
        "raw": raw,
        "wind_dir": None,
        "wind_speed": None,
        "wind_gust": None,
        "wind_dir_from": None,
        "wind_dir_to": None,
        "visibility": None,
        "visibility_greater_less": None,
        "temperature": None,
        "dew_point": None,
        "altimeter": None,
        "condition": None,
        "clouds.type": [],
        "clouds.ceiling": [],
        "clouds.additional_type": []
    }

    try:
        obs = Metar.Metar(raw)
    except Metar.ParserError as e:
        logger.error(f"Metar.ParserError: {e} for {raw}")
        return output

    if obs.wind_dir:
        output["wind_dir"] = round(obs.wind_dir.value())
    if obs.wind_speed:
        output["wind_speed"] = round(obs.wind_speed.value("KT"))
    if obs.wind_gust:
        output["wind_gust"] = round(obs.wind_gust.value("KT"))
    if obs.wind_dir_from:
        output["wind_dir_from"] = round(obs.wind_dir_from.value())
    if obs.wind_dir_to:
        output["wind_dir_to"] = round(obs.wind_dir_to.value())
    if obs.vis:
        output["visibility"] = round(obs.vis.value("M"))
    if obs.vis:
        output["visibility_greater_less"] = obs.vis._gtlt
    if obs.temp:
        output["temperature"] = round(obs.temp.value("C"))
    if obs.dewpt:
        output["dew_point"] = round(obs.dewpt.value("C"))
    if obs.press:
        output["altimeter"] = round(obs.press.value("HPA"))

    condition: str = ""

    if len(obs.weather) > 0:
        for weather in obs.weather[0]:
            condition += weather or ""

    output["condition"] = condition

    clouds_type = []
    clouds_ceiling = []
    clouds_additional_type = []

    if len(obs.sky) > 0:
        for cloud in obs.sky:
            if len(cloud) < 3:
                continue
            clouds_type.append(cloud[0])
            clouds_ceiling.append(round(cloud[1].value("FT")) if cloud[1] else None)
            clouds_additional_type.append(cloud[2])

    output["clouds.type"] = clouds_type
    output["clouds.ceiling"] = clouds_ceiling
    output["clouds.additional_type"] = clouds_additional_type

    return output
