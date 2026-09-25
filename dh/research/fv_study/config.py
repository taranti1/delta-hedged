"""Study configuration (every number that shapes the results lives here)."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parents[3]
DATA_DIR = REPO / "data" / "external" / "bitstamp"
CACHE_DIR = REPO / "data" / "cache" / "fv_study"
BULK_FILE = "btcusd_bitstamp_1min_2012-2025.csv.gz"
LATEST_FILE = "btcusd_bitstamp_1min_latest.csv"
BULK_URL = "https://raw.githubusercontent.com/ff137/bitstamp-btcusd-minute-data/main/data/historical/btcusd_bitstamp_1min_2012-2025.csv.gz"

SEED = 20260925


def utc(s: str) -> int:
    """'YYYY-MM-DD' -> UTC epoch seconds."""
    return int(dt.datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc).timestamp())


@dataclass(frozen=True)
class StudyConfig:
    # data
    data_start: str = "2022-12-01"  # warm-up for the first training window
    # decision times (minutes before the settlement time T)
    taus_min: tuple[int, ...] = (60, 45, 30, 20, 15, 10, 5, 3, 2)
    # EWMA half-lives tested (minutes)
    half_lives_min: tuple[int, ...] = (10, 30, 120, 360, 1440)
    ref_half_life_min: int = 120  # reference vol that places the strike grids (not fitted)
    # strike grids
    z_grid: tuple[float, ...] = tuple(float(x) for x in np.round(np.arange(-3.0, 3.0001, 0.25), 2))
    tail_abs_z: tuple[float, ...] = tuple(float(x) for x in np.round(np.arange(1.5, 3.5001, 0.25), 2))
    dollar_step: float = 250.0
    dollar_max_sd: float = 4.0
    # walk-forward
    train_months: int = 12
    periods: tuple[tuple[str, str, str], ...] = (
        ("validation", "2024-01-01", "2025-01-01"),
        ("test", "2025-01-01", "2026-09-25"),
    )
    # data hygiene
    outage_run_min: int = 5  # zero-volume flat runs this long are outages
    # seasonal profile used by the main models (chosen on the validation period, see Q3)
    seasonal_layout: str = "hour_of_week"
    seasonal_bucket_s: int = 3600
    seasonal_tz: str = "UTC"
    seasonal_method: str = "sq"
    seasonal_shrink: float = 0.0
    seasonal_norm_half_life_min: int = 4320  # slow causal EWMA (3 days) removing the vol level
    # bootstrap
    n_boot: int = 1000
    # FOMC statement dates (14:00 ET); the hour ending 15:00 ET contains the reaction
    fomc_dates: tuple[str, ...] = (
        "2024-01-31", "2024-03-20", "2024-05-01", "2024-06-12", "2024-07-31", "2024-09-18",
        "2024-11-07", "2024-12-18",
        "2025-01-29", "2025-03-19", "2025-05-07", "2025-06-18", "2025-07-30", "2025-09-17",
        "2025-10-29", "2025-12-10",
        "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17", "2026-07-29", "2026-09-16",
    )
    seasonal_layouts: tuple[tuple[str, int, str], ...] = field(
        default=(
            ("flat", 3600, "UTC"),
            ("time_of_day", 3600, "UTC"),
            ("day_type", 3600, "UTC"),
            ("day_type", 3600, "America/New_York"),
            ("day_type", 1800, "America/New_York"),
            ("hour_of_week", 3600, "UTC"),
            ("hour_of_week", 3600, "America/New_York"),
            ("hour_of_week", 1800, "America/New_York"),
        )
    )

    @property
    def taus_s(self) -> np.ndarray:
        return np.asarray(self.taus_min, dtype=np.int64) * 60


CFG = StudyConfig()
