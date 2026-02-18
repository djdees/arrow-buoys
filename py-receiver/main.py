"""
py-receiver: Fetches Arrow IPC stream from go-source,
loads into Polars, and produces Matplotlib charts.
"""

import io
import os
import time

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend — must be set before pyplot import
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import polars as pl
import pyarrow as pa
import requests

SOURCE_URL: str = os.environ.get("SOURCE_URL", "http://go-source:8080/stream")
OUT_DIR: str = os.environ.get("OUT_DIR", "/app/out")


def fetch_arrow_table(url: str, retries: int = 30, delay: float = 2.0) -> pa.Table:
    """Fetch Arrow IPC stream with retry logic (waits for go-source to be ready)."""
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            reader = pa.ipc.open_stream(io.BytesIO(resp.content))
            return reader.read_all()
        except Exception as exc:
            last_err = exc
            print(f"[attempt {attempt}/{retries}] Connection failed: {exc}. Retrying in {delay}s…")
            time.sleep(delay)
    raise RuntimeError(f"Failed fetching Arrow stream from {url}: {last_err}")


def save_winds(df: pl.DataFrame, out_path: str) -> None:
    """Line chart: wind speed (m/s) per station over time."""
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    d = df.filter(pl.col("wspd_ms").is_not_null()).sort(["station_id", "time"])
    stations = d["station_id"].unique().sort().to_list()

    fig, ax = plt.subplots(figsize=(12, 6))
    for s in stations:
        sub = d.filter(pl.col("station_id") == s)
        ax.plot(sub["time"].to_list(), sub["wspd_ms"].to_list(), label=s, linewidth=1.5)

    ax.set_title("Wind Speed (m/s) — Recent Observations by Station", fontsize=14)
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Wind Speed (m/s)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
    fig.autofmt_xdate()
    ax.legend(loc="upper right")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    print(f"Saved: {out_path}")


def save_wtmp(df: pl.DataFrame, out_path: str) -> None:
    """Bar chart: latest sea surface temperature (°C) per station."""
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    latest = (
        df.filter(pl.col("wtmp_c").is_not_null())
        .sort(["station_id", "time"])
        .group_by("station_id")
        .tail(1)
        .sort("wtmp_c", descending=True)
    )

    if latest.height == 0:
        print("WARN: no sea surface temperature data available — skipping wtmp chart.")
        return

    stations = latest["station_id"].to_list()
    temps = latest["wtmp_c"].to_list()

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(stations, temps, color="#3b82f6", width=0.5)
    ax.set_title("Latest Sea Surface Temperature (°C) by Station", fontsize=14)
    ax.set_ylabel("Temperature (°C)")
    ax.set_ylim(0, max(temps) * 1.15 if temps else 30)

    for bar, val in zip(bars, temps):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.15,
            f"{val:.1f}°C",
            ha="center",
            va="bottom",
            fontsize=10,
            fontweight="bold",
        )

    fig.tight_layout()
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    print(f"Saved: {out_path}")


def main() -> None:
    print(f"[py-receiver] Connecting to: {SOURCE_URL}")
    print(f"[py-receiver] Output dir:    {OUT_DIR}")

    tbl = fetch_arrow_table(SOURCE_URL)
    print(f"[py-receiver] Received table: {tbl.num_rows} rows, {tbl.num_columns} columns")
    print(f"[py-receiver] Schema: {tbl.schema}")

    # Convert to Polars; parse epoch-seconds timestamp into datetime
    df = pl.from_arrow(tbl).with_columns(
        pl.from_epoch("time", time_unit="s").alias("time")
    )

    print(df.head(10))
    print(f"\nStations: {df['station_id'].unique().sort().to_list()}")

    save_winds(df, f"{OUT_DIR}/winds_line.png")
    save_wtmp(df, f"{OUT_DIR}/wtmp_bar.png")

    print(f"\n[py-receiver] Done. Charts written to {OUT_DIR}/")


if __name__ == "__main__":
    main()
