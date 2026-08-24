"""
Always-on scheduler entrypoint for the yearly ingest.

This is the long-running container command. It runs the idempotent ingest every
Sunday at 06:00 (Europe/Stockholm). The ingest can also be run one-shot, outside
the schedule, via `python -m api_ingest`.

It also reports its own lifecycle to Discord: "online" on start, and "offline"
when the container is stopped (SIGTERM from `docker stop`, or SIGINT). So if the
scheduler dies and later comes back, both transitions are visible. A hard kill
(SIGKILL, OOM) can't be caught, so there you'd see only the next "online".
"""

import logging
import signal
import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from api_ingest import run_ingest
from src.config import load_config
from src.discord_notifier import DiscordNotifier

logger = logging.getLogger(__name__)

SCHEDULE_TIMEZONE = "Europe/Stockholm"
SCHEDULE_DAY_OF_WEEK = "sun"
SCHEDULE_HOUR = 6
SCHEDULE_MINUTE = 0

SCHEDULE_DESCRIPTION = f"every Sunday at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} {SCHEDULE_TIMEZONE}"


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )

    notifier = DiscordNotifier(load_config().discord_webhook_url)
    notify_offline_on_shutdown(notifier)
    notifier.online(f"Scheduler started — running the ingest {SCHEDULE_DESCRIPTION}.")

    scheduler = BlockingScheduler(timezone=SCHEDULE_TIMEZONE)
    scheduler.add_job(
        run_ingest,
        CronTrigger(day_of_week=SCHEDULE_DAY_OF_WEEK, hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE),
        id="weekly-ingest",
        misfire_grace_time=3600,
        coalesce=True,
    )

    logger.info("Scheduler started — weekly ingest %s", SCHEDULE_DESCRIPTION)
    scheduler.start()


def notify_offline_on_shutdown(notifier: DiscordNotifier) -> None:
    """Send an 'offline' notification when the container is asked to stop."""
    def handle(signum, _frame):
        name = signal.Signals(signum).name
        logger.info("Received %s — shutting down", name)
        notifier.offline(f"Scheduler stopped ({name}).")
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle)
    signal.signal(signal.SIGINT, handle)


if __name__ == "__main__":
    main()
