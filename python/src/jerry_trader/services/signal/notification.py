"""Email notification for SignalEngine triggers.

Triggered by Event.notify config embedded in events.yaml.
Checks extra conditions, catalyst flag, and per-event cooldown before sending.
"""

from __future__ import annotations

import json
import os
import smtplib
import time
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any

import redis

from jerry_trader.domain.event import Event, NotifyConfig
from jerry_trader.shared.ids.redis_keys import news_catalyst_flag, static_ticker_summary
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("notification", log_to_file=True)


@dataclass
class SMTPConfig:
    host: str = "smtp.gmail.com"
    port: int = 587
    username_env: str = "GMAIL_USERNAME"
    password_env: str = "GMAIL_APP_PASSWORD"


class NotificationManager:
    """Checks event.notify and sends email when a signal fires."""

    def __init__(
        self,
        redis_client: redis.Redis,
        session_id: str,
        smtp_config: SMTPConfig | None = None,
        market_redis: redis.Redis | None = None,
    ):
        self._redis = redis_client
        self._session_id = session_id
        self._smtp = smtp_config
        self._market_redis = market_redis
        self._last_sent: dict[tuple[str, str], float] = {}

    def evaluate(
        self,
        event: Event,
        symbol: str,
        timeframe: str,
        factors: dict[str, float],
        price: float | None = None,
        ml_result: Any = None,
        extra: dict[str, Any] | None = None,
    ) -> bool:
        """Check event.notify and send email if all conditions pass.

        Returns True if an email was sent.
        """
        notify = event.notify
        if notify is None or not notify.enabled or not self._smtp:
            return False

        # Extra factor conditions (AND'd with the event's own conditions)
        for cond in notify.extra_conditions:
            val = factors.get(cond.factor)
            if not cond.check(val):
                return False

        # Catalyst check
        if notify.catalysts and not self._check_catalyst(symbol):
            return False

        # Cooldown
        if not self._check_cooldown(event.name, symbol, notify.cooldown_sec):
            return False

        if self._send_email(
            notify, event.name, symbol, timeframe, price, factors, ml_result, extra
        ):
            self._last_sent[(event.name, symbol)] = time.time()
            return True
        return False

    def _check_catalyst(self, symbol: str) -> bool:
        key = news_catalyst_flag(self._session_id, symbol)
        try:
            return self._redis.get(key) == b"1"
        except Exception:
            return False

    def _fetch_static_data(self, symbol: str) -> dict[str, str]:
        """Fetch float, marketCap, etc. from Redis static ticker summary."""
        try:
            key = static_ticker_summary(self._session_id, symbol)
            data = self._redis.hgetall(key)
            if data:
                return {
                    k.decode() if isinstance(k, bytes) else k: (
                        v.decode() if isinstance(v, bytes) else v
                    )
                    for k, v in data.items()
                }
        except Exception:
            pass
        return {}

    def _fetch_market_data(self, symbol: str) -> dict[str, str]:
        """Fetch latest market data (price, change%, volume) from snapshot stream.

        The stream stores entries as ``{timestamp, data: "[{symbol, price, ...}]"}``
        where ``data`` is a JSON array of per-ticker snapshot objects.

        Uses XREVRANGE to scan recent entries for the matching symbol.
        Falls back to empty dict on any error.
        """
        if not self._market_redis:
            return {}
        try:
            from jerry_trader.shared.ids.redis_keys import market_snapshot_processed

            stream_key = market_snapshot_processed(self._session_id)
            # Scan last 200 entries for the matching symbol
            results = self._market_redis.xrevrange(stream_key, count=200)
            if not results:
                return {}

            for _, entry in results:
                # The entry has "timestamp" and "data" fields.
                # "data" is a JSON array of snapshot objects.
                raw_data = entry.get("data", entry.get(b"data", b""))
                if isinstance(raw_data, bytes):
                    raw_data = raw_data.decode()
                if not raw_data:
                    continue

                try:
                    snapshots = json.loads(raw_data)
                except (json.JSONDecodeError, TypeError):
                    continue

                for snap in snapshots:
                    snap_symbol = str(snap.get("symbol", "")).upper()
                    if snap_symbol != symbol.upper():
                        continue

                    def _val(key: str) -> str:
                        v = snap.get(key, "")
                        return str(v) if v is not None else ""

                    return {
                        "price": _val("price"),
                        "change_pct": _val("changePercent"),
                        "volume": _val("volume"),
                    }
        except Exception:
            pass
        return {}

    @staticmethod
    def _fmt_human(value: str) -> str:
        """Convert raw number string to human-readable (26257615 → 26.26M)."""
        try:
            n = float(value)
        except (ValueError, TypeError):
            return value
        if abs(n) >= 1_000_000_000:
            return f"{n/1_000_000_000:.2f}B"
        elif abs(n) >= 1_000_000:
            return f"{n/1_000_000:.2f}M"
        elif abs(n) >= 1_000:
            return f"{n/1_000:.2f}K"
        else:
            return f"{n:.2f}"

    def _check_cooldown(self, event_name: str, symbol: str, cooldown_sec: int) -> bool:
        last = self._last_sent.get((event_name, symbol), 0)
        return (time.time() - last) >= cooldown_sec

    def _send_email(
        self,
        notify: NotifyConfig,
        event_name: str,
        symbol: str,
        timeframe: str,
        price: float | None,
        factors: dict[str, float],
        ml_result: Any,
        extra: dict[str, Any] | None = None,
    ) -> bool:
        if not self._smtp:
            return False

        try:
            from_addr = os.getenv(self._smtp.username_env, "")
            password = os.getenv(self._smtp.password_env, "")

            if not from_addr or not password:
                logger.error(
                    f"Notification: missing SMTP credentials "
                    f"({self._smtp.username_env}, {self._smtp.password_env})"
                )
                return False

            subject, body = self._render_templates(
                notify, event_name, symbol, timeframe, price, factors, ml_result, extra
            )

            msg = EmailMessage()
            msg["Subject"] = subject
            msg["From"] = from_addr
            msg["To"] = from_addr
            msg.set_content(body)

            with smtplib.SMTP(self._smtp.host, self._smtp.port, timeout=10) as smtp:
                smtp.starttls()
                smtp.login(from_addr, password)
                smtp.send_message(msg)

            # Log the full email content as one block (like NewsProcessor)
            sep = "=" * 50
            logger.info(
                f"\n{sep}\n"
                f"Notification sent: {event_name} / {symbol}\n"
                f"Subject: {subject}\n"
                f"{body}"
                f"{sep}"
            )
            return True

        except Exception as e:
            logger.error(
                f"Notification send failed: event={event_name} symbol={symbol} - {e}"
            )
            return False

    def _render_templates(
        self,
        notify: NotifyConfig,
        event_name: str,
        symbol: str,
        timeframe: str,
        price: float | None,
        factors: dict[str, float],
        ml_result: Any,
        extra: dict[str, Any] | None = None,
    ) -> tuple[str, str]:
        # Fetch static data (float, marketCap, etc.)
        static_data = self._fetch_static_data(symbol)

        vars: dict[str, Any] = {
            "event_name": event_name,
            "symbol": symbol,
            "timeframe": timeframe,
            "price": f"{price:.4f}" if price is not None else "N/A",
            "factors": ", ".join(f"{k}={v:.4f}" for k, v in factors.items()),
            # Defaults for optional fields — overridden if available
            "change_pct": "N/A",
            "volume": "N/A",
            "float": "N/A",
            "marketCap": "N/A",
            "sector": "N/A",
        }
        vars.update(static_data)
        vars.update(factors)

        # Merge extra context (catalyst news details, etc.)
        if extra:
            vars.update(extra)

        # Merge market data (price, change%, volume from snapshot stream)
        market_data = self._fetch_market_data(symbol)
        if market_data:
            vars.update(market_data)

        # Format numeric fields as human-readable
        for key in ("float", "marketCap", "volume"):
            raw = vars.get(key, "N/A")
            if raw and raw != "N/A":
                vars[key] = self._fmt_human(str(raw))
        # Round change_pct to 2 decimal places
        change = vars.get("change_pct", "N/A")
        if change and change != "N/A":
            try:
                vars["change_pct"] = f"{float(change):.2f}"
            except (ValueError, TypeError):
                pass

        # Format borrow_fee from decimal (0.35) to percentage (35.00%)
        borrow_fee = vars.get("borrow_fee", "N/A")
        if borrow_fee and borrow_fee != "N/A":
            try:
                vars["borrow_fee"] = f"{float(borrow_fee) * 100:.2f}%"
            except (ValueError, TypeError):
                pass

        if ml_result is not None:
            vars["ml_expected_return"] = getattr(ml_result, "expected_return", "N/A")
            vars["ml_confidence"] = getattr(ml_result, "confidence", "N/A")

        try:
            subject = notify.subject_template.format(**vars)
        except (KeyError, ValueError) as e:
            logger.error(f"Bad subject template for event '{event_name}': {e}")
            subject = f"JerryTrader: {event_name} on {symbol}"

        try:
            body = notify.body_template.format(**vars)
        except (KeyError, ValueError) as e:
            logger.error(f"Bad body template for event '{event_name}': {e}")
            body = f"Symbol: {symbol}\nEvent: {event_name}\nPrice: {vars['price']}\n{vars['factors']}"

        return subject, body
