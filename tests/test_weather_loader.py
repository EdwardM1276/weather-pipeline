from loading import load_weather_to_rds


def test_extract_daily_rows_from_payload():
    payload = {
        "daily": {
            "time": ["2026-09-23", "2026-09-24"],
            "temperature_2m_max": [26.5, 25.3],
            "temperature_2m_min": [17.8, 19.9],
            "precipitation_sum": [0.3, 0.0],
            "wind_speed_10m_max": [13.0, 16.4],
        }
    }

    rows = load_weather_to_rds.extract_daily_rows(payload, "durban", "durban_20260923.json")

    assert rows[0]["city"] == "durban"
    assert rows[0]["measurement_date"] == "2026-09-23"
    assert rows[0]["temperature_2m_max"] == 26.5
    assert rows[1]["precipitation_sum"] == 0.0
    assert rows[1]["source_file"] == "durban_20260923.json"
