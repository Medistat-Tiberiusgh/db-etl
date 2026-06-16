"""
Discord webhook notifications for the yearly ingest.

Mirrors the embed format used in the it-pub-notifier project: a titled, coloured
embed with an ISO timestamp, and separate success / error / info helpers.
"""

import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

GREEN = 0x00FF00
RED = 0xFF0000
BLUE = 0x0099FF


class DiscordNotifier:
    """Posts embed messages to a Discord webhook, if one is configured."""

    def __init__(self, webhook_url: str | None) -> None:
        self._webhook_url = webhook_url

    def success(self, description: str, fields: list[dict] | None = None) -> None:
        self._send("📊 New prescription data ingested", description, GREEN, fields)

    def error(self, description: str, fields: list[dict] | None = None) -> None:
        self._send("❌ Prescription ingest failed", description, RED, fields)

    def info(self, description: str, fields: list[dict] | None = None) -> None:
        self._send("ℹ️ Prescription ingest", description, BLUE, fields)

    def _send(
        self,
        title: str,
        description: str,
        color: int,
        fields: list[dict] | None = None,
    ) -> None:
        """Post one embed; a missing webhook or a failed post never raises."""
        if not self._webhook_url:
            logger.info("Discord webhook not configured; skipping: %s", description)
            return

        embed = {
            "title": title,
            "description": description,
            "color": color,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if fields:
            embed["fields"] = fields

        try:
            response = requests.post(
                self._webhook_url,
                json={"embeds": [embed]},
                timeout=15,
            )
            response.raise_for_status()
        except requests.RequestException as error:
            # A notification failure must not break the ingest.
            logger.warning("Failed to send Discord notification: %s", error)
