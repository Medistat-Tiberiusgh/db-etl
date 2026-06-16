"""
Always-on scheduler entrypoint for the yearly ingest.

This is the long-running container command. It runs the idempotent ingest every
Sunday at 03:30 (Europe/Stockholm). The ingest can also be run one-shot, outside
the schedule, via `python -m api_ingest`.
"""

import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from api_ingest import run_ingest

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )

    scheduler = BlockingScheduler(timezone="Europe/Stockholm")
    scheduler.add_job(
        run_ingest,
        CronTrigger(day_of_week="sun", hour=3, minute=30),
        id="weekly-ingest",
        # If the container was down at 03:30, still run when it comes back
        # within the hour, and collapse multiple missed runs into one.
        misfire_grace_time=3600,
        coalesce=True,
    )

    logger.info("Scheduler started — weekly ingest every Sunday 03:30 Europe/Stockholm")
    scheduler.start()


if __name__ == "__main__":
    main()
