"""Email notification for SignalEngine triggers.

Triggered by Event.notify config embedded in events.yaml.
Checks extra conditions, catalyst flag, and per-event cooldown before sending.
"""

from __future__ import annotations

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

        Uses XREVRANGE on market_snapshot_processed stream to find the most
        recent entry for *symbol*. Falls back to empty dict on any error.
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
                # Handle both bytes and str keys (depends on decode_responses)
                entry_symbol = entry.get("symbol", entry.get(b"symbol", b""))
                if isinstance(entry_symbol, bytes):
                    entry_symbol = entry_symbol.decode()
                if entry_symbol.upper() != symbol.upper():
                    continue

                def _val(key: str) -> str:
                    v = entry.get(key, entry.get(key.encode(), b""))
                    if isinstance(v, bytes):
                        v = v.decode()
                    return v

                return {
                    "price": _val("price"),
                    "change_pct": _val("changePercent"),
                    "volume": _val("volume"),
                }
        except Exception:
            pass
        return {}

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

            logger.info(f"Notification sent: event={event_name} symbol={symbol}")
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
            **static_data,
        }
        vars.update(factors)

        # Merge extra context (catalyst news details, etc.)
        if extra:
            vars.update(extra)

        # Merge market data (price, change%, volume from snapshot stream)
        market_data = self._fetch_market_data(symbol)
        if market_data:
            vars.update(market_data)

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
