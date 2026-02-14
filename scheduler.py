"""
Weekly scheduler: runs the news-trends pipeline every Sunday at 21:00 Asia/Riyadh (UTC+3).
Usage: python scheduler.py
"""

import datetime
import time
import zoneinfo

import schedule

from main import main

RIYADH_TZ = zoneinfo.ZoneInfo("Asia/Riyadh")


def run_pipeline() -> None:
    print(f"[{datetime.datetime.now(RIYADH_TZ):%Y-%m-%d %H:%M:%S %Z}] Starting pipeline...")
    try:
        main()
        print(f"[{datetime.datetime.now(RIYADH_TZ):%Y-%m-%d %H:%M:%S %Z}] Pipeline finished.")
    except Exception as exc:
        print(f"[{datetime.datetime.now(RIYADH_TZ):%Y-%m-%d %H:%M:%S %Z}] Pipeline error: {exc}")


def riyadh_to_local(hour: int, minute: int) -> str:
    """Convert a Riyadh wall-clock time to the local system's equivalent HH:MM string."""
    now_riyadh = datetime.datetime.now(RIYADH_TZ)
    target_riyadh = now_riyadh.replace(hour=hour, minute=minute, second=0, microsecond=0)
    target_local = target_riyadh.astimezone()
    return f"{target_local:%H:%M}"


if __name__ == "__main__":
    local_time = riyadh_to_local(9, 0)
    schedule.every().sunday.at(local_time).do(run_pipeline)

    next_run = schedule.next_run()
    print(f"Scheduler started. Next run: {next_run:%A %Y-%m-%d %H:%M} (local)")
    print(f"  = every Sunday at 09:00 Asia/Riyadh")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nScheduler stopped.")
