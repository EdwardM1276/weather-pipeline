"""Phase 3: Data quality gate.

Every record must earn its way into the curated layer. Validation happens
on RAW values (before type conversion) so that broken input can never be
'laundered' into looking acceptable by a lenient transformation.

Design rules:
  - collect ALL violations per batch, then fail once with the full report
  - no silent fixing: bad data raises, it does not get skipped
"""

from datetime import datetime

# Physical plausibility bounds for South Africa.
# Values outside these are almost certainly sensor/API corruption,
# not weather. (AUH: Addis Ababa-like extremes are outside SA.)
TEMP_MIN_BOUND_C = -50.0
TEMP_MAX_BOUND_C = 60.0


class DataValidationError(Exception):
    """Raised when one or more records fail validation."""

    def __init__(self, violations: list):
        self.violations = violations
        report = "\n".join(f"  - {v}" for v in violations)
        super().__init__(f"Validation failed with {len(violations)} violation(s):\n{report}")


REQUIRED_FIELDS = ["city", "date", "temp_max_c", "temp_min_c", "precipitation_mm", "wind_max_kmh"]


def _is_valid_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except (ValueError, TypeError):
        return False


def _to_float(value):
    """Return float or None if not parseable (None = violation, not crash)."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def validate_records(records: list) -> list:
    """Validate a batch of raw row-dicts. Returns the same list if clean.

    Raises DataValidationError with the complete violation report otherwise.
    """
    violations = []

    for i, rec in enumerate(records):
        tag = f"record #{i} ({rec.get('city', '?')} {rec.get('date', '?')})"

        # --- Rule 1: required fields present and not null ---
        for field in REQUIRED_FIELDS:
            if field not in rec or rec[field] is None:
                violations.append(f"{tag}: missing/null field '{field}'")
                continue

        # If basics are broken, skip the numeric checks for this record.
        if any(rec.get(f) is None for f in REQUIRED_FIELDS):
            continue

        # --- Rule 2: date well-formed ---
        if not _is_valid_date(rec["date"]):
            violations.append(f"{tag}: malformed date '{rec['date']}'")

        # --- Rule 3: numerics parse and are physically plausible ---
        tmax = _to_float(rec["temp_max_c"])
        tmin = _to_float(rec["temp_min_c"])
        precip = _to_float(rec["precipitation_mm"])
        wind = _to_float(rec["wind_max_kmh"])

        if tmax is None:
            violations.append(f"{tag}: temp_max_c not numeric: {rec['temp_max_c']!r}")
        if tmin is None:
            violations.append(f"{tag}: temp_min_c not numeric: {rec['temp_min_c']!r}")
        if precip is None:
            violations.append(f"{tag}: precipitation_mm not numeric: {rec['precipitation_mm']!r}")
        if wind is None:
            violations.append(f"{tag}: wind_max_kmh not numeric: {rec['wind_max_kmh']!r}")

        # Cross-field rules only make sense if both values parsed.
        if tmax is not None and tmin is not None:
            # Rule 4: plausible absolute range
            for name, val in (("temp_max_c", tmax), ("temp_min_c", tmin)):
                if not (TEMP_MIN_BOUND_C <= val <= TEMP_MAX_BOUND_C):
                    violations.append(
                        f"{tag}: {name}={val} outside plausible range "
                        f"[{TEMP_MIN_BOUND_C}, {TEMP_MAX_BOUND_C}]"
                    )
            # Rule 5: internal consistency — the classic API corruption
            if tmax < tmin:
                violations.append(
                    f"{tag}: temp_max_c ({tmax}) < temp_min_c ({tmin})"
                )

        # Rule 6: non-negative physical quantities
        if precip is not None and precip < 0:
            violations.append(f"{tag}: negative precipitation_mm ({precip})")
        if wind is not None and wind < 0:
            violations.append(f"{tag}: negative wind_max_kmh ({wind})")

    if violations:
        raise DataValidationError(violations)

    return records