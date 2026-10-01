"""Prove the validation gate catches corruption. Run: python tests/test_validation.py"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transformation.transform import clean_records, reshape_payload
from transformation.validate import DataValidationError, validate_records

# --- A realistic raw payload, then we POISON it in four different ways ---
GOOD_DAILY = {
    "time": ["2026-09-24", "2026-09-25"],
    "temperature_2m_max": [22.1, 21.8],
    "temperature_2m_min": [13.2, 12.9],
    "precipitation_sum": [0.0, 3.2],
    "wind_speed_10m_max": [18.5, 24.0],
}


def make_payload(daily: dict) -> dict:
    return {"daily": daily}


def expect_failure(name: str, daily: dict, must_mention: str):
    records = reshape_payload(make_payload(daily), "test_city")
    try:
        validate_records(records)
    except DataValidationError as e:
        assert must_mention in str(e), f"[{name}] failed but for the WRONG reason:\n{e}"
        print(f"[PASS] {name}: caught -> {must_mention}")
        return
    raise AssertionError(f"[FAIL] {name}: validator let bad data through!")


def expect_success(name: str, daily: dict):
    records = reshape_payload(make_payload(daily), "test_city")
    validate_records(records)
    cleaned = clean_records(records)
    assert isinstance(cleaned[0]["temp_max_c"], float)
    assert cleaned[0]["temp_range_c"] == round(22.1 - 13.2, 2)
    print(f"[PASS] {name}: clean data flowed through, derived field correct")


# Attack 1: max < min (the classic API field-swap corruption)
expect_failure(
    "max below min",
    {**GOOD_DAILY, "temperature_2m_max": [10.0, 21.8], "temperature_2m_min": [13.2, 12.9]},
    "temp_max_c (10.0) < temp_min_c (13.2)",
)

# Attack 2: negative precipitation
expect_failure(
    "negative precipitation",
    {**GOOD_DAILY, "precipitation_sum": [-2.5, 3.2]},
    "negative precipitation_mm (-2.5)",
)

# Attack 3: impossible temperature
expect_failure(
    "impossible temperature",
    {**GOOD_DAILY, "temperature_2m_max": [95.0, 21.8]},
    "outside plausible range",
)

# Attack 4: non-numeric garbage
expect_failure(
    "non-numeric value",
    {**GOOD_DAILY, "wind_speed_10m_max": ["N/A", 24.0]},
    "not numeric",
)

# Attack 5: malformed date
expect_failure(
    "bad date",
    {**GOOD_DAILY, "time": ["24/09/2026", "2026-09-25"]},
    "malformed date",
)

# And the happy path must still work end to end
expect_success("good data", GOOD_DAILY)

print("\nAll validation tests passed.")