"""Central configuration for the weather pipeline.

Rule of thumb: anything that could change (city list, URLs, file paths)
lives HERE, not buried inside script logic. When Phase 5 (Airflow) reuses
these scripts, it imports from this one place.
"""

# --- Open-Meteo API ---
OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"

# The daily variables we ask Open-Meteo for.
# These names become our columns later, so define them once, here.
DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "wind_speed_10m_max",
]

# (latitude, longitude) — the API takes coordinates, not city names.
CITIES = {
    "durban": (-29.85, 31.02),
    "cape_town": (-33.92, 18.42),
    "johannesburg": (-26.20, 28.05),
    "pretoria": (-25.75, 28.19),
}

# Ask for dates in South African time so "daily" means a SA calendar day.
TIMEZONE = "Africa/Johannesburg"
FORECAST_DAYS = 7  # how many days ahead to pull

# --- Local storage (Phase 1). Phase 2 replaces this with S3. ---
RAW_DATA_DIR = "data/raw"

# --- AWS S3 (Phase 2) ---
S3_BUCKET = "za-weather-raw-data-edward" 
S3_KEY_PREFIX = "raw"                     