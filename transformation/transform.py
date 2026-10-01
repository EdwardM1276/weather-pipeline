"""Phase 3: Raw -> typed, analysis-ready records.

Runs ONLY after validation passes. Converts string values to proper types,
normalizes dates, and derives new fields. Pure functions: input list of
dicts -> output list of dicts. No I/O, no S3, no globals — which makes
this trivially testable in Phase 3's test step.
"""


def reshape_payload(payload: dict, city_name: str) -> list:
    """Transpose Open-Meteo's column-oriented 'daily' block into row-dicts.

    This is STRUCTURAL reshaping only — values stay as-is (strings/numbers),
    because validation must see what actually arrived.
    """
    daily = payload["daily"]
    dates = daily["time"]
    n = len(dates)

    # Defensive: parallel arrays must be equal length or the payload is broken.
    for field in ("temperature_2m_max", "temperature_2m_min",
                  "precipitation_sum", "wind_speed_10m_max"):
        if len(daily[field]) != n:
            raise ValueError(
                f"Payload structure error: 'daily.{field}' has "
                f"{len(daily[field])} values but 'time' has {n}"
            )

    records = []
    for i in range(n):
        records.append({
            "city": city_name,
            "date": dates[i],                          # still a string, e.g. "2026-09-24"
            "temp_max_c": daily["temperature_2m_max"][i],
            "temp_min_c": daily["temperature_2m_min"][i],
            "precipitation_mm": daily["precipitation_sum"][i],
            "wind_max_kmh": daily["wind_speed_10m_max"][i],
        })
    return records


def clean_records(records: list) -> list:
    """Convert validated raw records into typed, enriched records.

    Steps: string -> float, ISO date normalization, derived field.
    """
    cleaned = []
    for rec in records:
        tmax = float(rec["temp_max_c"])
        tmin = float(rec["temp_min_c"])

        cleaned.append({
            "city": rec["city"],
            "date": rec["date"],                # already "YYYY-MM-DD"; ISO is the normalized form
            "temp_max_c": tmax,
            "temp_min_c": tmin,
            "temp_range_c": round(tmax - tmin, 2),   # DERIVED FIELD (diagram: 'temp_range_c')
            "precipitation_mm": float(rec["precipitation_mm"]),
            "wind_max_kmh": float(rec["wind_max_kmh"]),
        })
    return cleaned