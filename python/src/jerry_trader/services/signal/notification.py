"""Email notification for SignalEngine triggers.

Triggered by Event.notify config embedded in events.yaml.
Checks extra conditions, catalyst flag, and per-event cooldown before sending.

Catalyst auto-diagnosis: when a catalyst notification fires, optionally spawns
a background ``claude -p`` headless invocation running /micro-cap-diagnosis.
The structured verdict is sent as a follow-up email.
"""

from __future__ import annotations

import json
import os
import smtplib
import subprocess
import threading
import time
import uuid as _uuid
from dataclasses import dataclass, field
from email.message import EmailMessage
from typing import Any

import redis

from jerry_trader.domain.event import Event, NotifyConfig
from jerry_trader.shared.ids.redis_keys import news_catalyst_flag, static_ticker_summary
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("notification", log_to_file=True)


def _html_escape(text: str) -> str:
    """Minimal HTML escape for email bodies."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


# ─────────────────────────────────────────────────────────────────────────────
# JSON Schema for /micro-cap-diagnosis structured output
# ─────────────────────────────────────────────────────────────────────────────
DIAGNOSIS_JSON_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "verdict": {
            "type": "string",
            "enum": ["pursue", "watch", "avoid"],
            "description": "Final trading recommendation",
        },
        "verdict_reason": {
            "type": "string",
            "description": "Concise one-line reason for the verdict",
        },
        "catalyst_quality": {
            "type": "string",
            "enum": ["genuine_catalyst", "narrative_pump", "neutral_unconfirmed"],
            "description": "News quality classification",
        },
        "tradability_score": {
            "type": "integer",
            "minimum": 1,
            "maximum": 5,
            "description": "Overall tradability score 1-5",
        },
        "float_assessment": {
            "type": "string",
            "enum": ["ideal", "tradable", "marginal", "unsuitable"],
            "description": "Float suitability for the user's low-float momentum strategy",
        },
        "red_flags": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Key red flags found (empty if clean)",
        },
        "key_risk": {
            "type": "string",
            "description": "Single biggest risk if entering this trade",
        },
    },
    "required": [
        "verdict",
        "verdict_reason",
        "catalyst_quality",
        "tradability_score",
        "red_flags",
        "key_risk",
    ],
})


@dataclass
class SMTPConfig:
    host: str = "smtp.gmail.com"
    port: int = 587
    username_env: str = "GMAIL_USERNAME"
    password_env: str = "GMAIL_APP_PASSWORD"


@dataclass
class DiagnosisConfig:
    """Configuration for auto-triggering /micro-cap-diagnosis via headless Claude Code.

    Attributes:
        enabled: When False, diagnosis is skipped entirely.
        cli_path: Path to the ``claude`` binary (default: ``claude`` on $PATH).
        timeout_sec: Hard timeout for the subprocess (includes web searches).
        bare_mode: Pass ``--bare`` to skip hooks/CLAUDE.md auto-discovery for
                   faster startup. The skill is loaded explicitly via ``--agents``.
        agents_json: JSON string for ``--agents`` flag when bare_mode is True.
        extra_args: Additional CLI arguments appended to the command.
    """

    enabled: bool = False
    cli_path: str = "claude"
    timeout_sec: int = 90
    bare_mode: bool = True
    extra_args: list[str] = field(default_factory=list)


class NotificationManager:
    """Checks event.notify and sends email when a signal fires.

    If *diagnosis_config* is enabled and the event includes catalyst ``extra``
    context, a background thread spawns ``claude -p /micro-cap-diagnosis`` to
    produce a structured verdict.  The verdict is delivered as a second email.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        session_id: str,
        smtp_config: SMTPConfig | None = None,
        market_redis: redis.Redis | None = None,
        diagnosis_config: DiagnosisConfig | None = None,
    ):
        self._redis = redis_client
        self._session_id = session_id
        self._smtp = smtp_config
        self._market_redis = market_redis
        self._diagnosis = diagnosis_config or DiagnosisConfig()
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

        sent, message_id = self._send_email(
            notify, event.name, symbol, timeframe, price, factors, ml_result, extra
        )
        if sent:
            self._last_sent[(event.name, symbol)] = time.time()

            # Auto-trigger /micro-cap-diagnosis for catalyst events
            if extra and extra.get("catalyst_title"):
                subject, body = self._render_templates(
                    notify, event.name, symbol, timeframe, price,
                    factors, ml_result, extra,
                )
                self._maybe_run_diagnosis(subject, body, message_id)

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

    # ── Catalyst auto-diagnosis ──────────────────────────────────────────────

    def _maybe_run_diagnosis(
        self,
        catalyst_subject: str,
        body: str,
        in_reply_to: str | None = None,
    ) -> None:
        """Spawn a background thread to run /micro-cap-diagnosis if enabled."""
        cfg = self._diagnosis
        if not cfg.enabled:
            return

        thread = threading.Thread(
            target=self._run_diagnosis_thread,
            args=(catalyst_subject, body, in_reply_to),
            daemon=True,
            name=f"diagnosis-{_uuid.uuid4().hex[:8]}",
        )
        thread.start()
        logger.info("Diagnosis: spawned background thread for catalyst")

    def _run_diagnosis_thread(
        self,
        catalyst_subject: str,
        body: str,
        in_reply_to: str | None = None,
    ) -> None:
        """Run ``claude -p /micro-cap-diagnosis`` headless, send verdict email.

        Runs in a **daemon thread** so a stuck subprocess never blocks shutdown.
        The *in_reply_to* Message-ID threads the diagnosis under the catalyst email.
        """
        cfg = self._diagnosis

        # ── Build the command ─────────────────────────────────────────────
        cmd: list[str] = [cfg.cli_path]

        if cfg.bare_mode:
            cmd.append("--bare")
            # Explicitly load the skill so bare mode still finds it
            agents_json = json.dumps({
                "micro-cap-diagnosis": {
                    "path": ".agents/skills/micro-cap-diagnosis/SKILL.md",
                }
            })
            cmd += ["--agents", agents_json]

        cmd += [
            "-p",
            "/micro-cap-diagnosis",
            "--output-format", "json",
            "--json-schema", DIAGNOSIS_JSON_SCHEMA,
            "--permission-mode", "bypassPermissions",
        ]

        if cfg.extra_args:
            cmd.extend(cfg.extra_args)

        logger.info(
            f"Diagnosis: spawning claude -p | "
            f"bare={cfg.bare_mode} timeout={cfg.timeout_sec}s"
        )

        try:
            proc = subprocess.run(
                cmd,
                input=body,
                capture_output=True,
                text=True,
                timeout=cfg.timeout_sec,
            )
        except subprocess.TimeoutExpired:
            logger.error(
                f"Diagnosis: claude -p timed out after {cfg.timeout_sec}s"
            )
            return
        except FileNotFoundError:
            logger.error(
                f"Diagnosis: claude binary not found at '{cfg.cli_path}'. "
                f"Is Claude Code installed?"
            )
            return
        except Exception as exc:
            logger.error(f"Diagnosis: subprocess failed: {exc}")
            return

        if proc.returncode != 0:
            stderr_summary = (
                proc.stderr[:400] if proc.stderr else "<no stderr>"
            )
            logger.error(
                f"Diagnosis: claude -p exited {proc.returncode}: "
                f"{stderr_summary}"
            )
            return

        # ── Parse structured output ───────────────────────────────────────
        try:
            result = json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            logger.error(
                f"Diagnosis: failed to parse JSON output: {exc}"
            )
            return

        structured = result.get("structured_output")
        if not structured:
            logger.error(
                "Diagnosis: no structured_output in response — "
                "the skill may not have produced valid output"
            )
            return

        logger.info(
            f"Diagnosis verdict: {structured.get('verdict')} "
            f"(score={structured.get('tradability_score')}) "
            f"for catalyst"
        )

        # ── Send the verdict email + persist outcome ─────────────────────
        self._send_diagnosis_email(catalyst_subject, structured, in_reply_to)

    # ── Label formatters ───────────────────────────────────────────────────

    _VERDICT_STYLE: dict[str, dict[str, str]] = {
        "pursue": {
            "emoji": "\U0001f7e2", "label": "PURSUE",
            "color": "#1a7d4c", "bg": "#e8f5e9",
        },
        "watch": {
            "emoji": "\U0001f7e1", "label": "WATCH",
            "color": "#b8860b", "bg": "#fff8e1",
        },
        "avoid": {
            "emoji": "\U0001f534", "label": "AVOID",
            "color": "#c62828", "bg": "#ffebee",
        },
    }

    _CATALYST_QUALITY_LABELS: dict[str, str] = {
        "genuine_catalyst": "Genuine Catalyst",
        "narrative_pump": "Narrative Pump",
        "neutral_unconfirmed": "Neutral / Unconfirmed",
    }

    _FLOAT_LABELS: dict[str, str] = {
        "ideal": "\U0001f7e2 Ideal (< 2M)",
        "tradable": "\U0001f7e1 Tradable (2M – 10M)",
        "marginal": "\U0001f7e0 Marginal (10M – 50M)",
        "unsuitable": "\U0001f534 Unsuitable (> 50M)",
    }

    # ═════════════════════════════════════════════════════════════════════════

    def _build_diagnosis_html(
        self,
        verdict_key: str,
        diagnosis: dict[str, Any],
        catalyst_subject: str,
    ) -> str:
        """Render the diagnosis as a responsive HTML email."""
        style = self._VERDICT_STYLE.get(verdict_key, self._VERDICT_STYLE["avoid"])
        score = diagnosis.get("tradability_score", "?")
        cat_quality_raw = diagnosis.get("catalyst_quality", "")
        cat_quality = self._CATALYST_QUALITY_LABELS.get(
            cat_quality_raw, cat_quality_raw.replace("_", " ").title()
        )
        float_raw = diagnosis.get("float_assessment", "")
        float_label = self._FLOAT_LABELS.get(float_raw, float_raw.title())
        reason = diagnosis.get("verdict_reason", "N/A")
        key_risk = diagnosis.get("key_risk", "N/A")
        red_flags: list[str] = diagnosis.get("red_flags", [])

        # Score stars
        stars = "★" * score + "☆" * (5 - score)

        # Red flag rows
        flag_rows: list[str] = []
        for i, flag in enumerate(red_flags):
            flag_rows.append(
                '<tr>'
                f'<td style="padding:5px 0;font-size:13px;color:#555;'
                f'border-bottom:1px solid #f0f0f0;">'
                f'<span style="color:{style["color"]};font-weight:700;">'
                f'&nbsp;#&nbsp;</span>{_html_escape(flag)}</td>'
                '</tr>'
            )
        flags_html = "\n".join(flag_rows) if flag_rows else (
            '<tr><td style="padding:5px 0;font-size:13px;color:#888;">'
            '(none)</td></tr>'
        )

        return f"""\
<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f5f5f5;
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<table cellpadding="0" cellspacing="0" border="0" width="100%"
  style="background:#f5f5f5;">
<tr><td align="center" style="padding:20px 12px;">

  <!-- Card -->
  <table cellpadding="0" cellspacing="0" border="0" width="100%"
    style="max-width:560px;background:#ffffff;border-radius:10px;
    overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.08);">

    <!-- Verdict banner -->
    <tr>
      <td style="background:{style['color']};padding:22px 24px;text-align:center;">
        <div style="font-size:44px;line-height:1;margin-bottom:8px;">
          {style['emoji']}</div>
        <div style="font-size:20px;font-weight:800;color:#ffffff;
          letter-spacing:1.2px;text-transform:uppercase;">
          {style['label']} &nbsp;&middot;&nbsp; {score}/5</div>
        <div style="font-size:12px;color:rgba(255,255,255,0.8);margin-top:4px;">
          {stars}</div>
      </td>
    </tr>

    <!-- Reason -->
    <tr>
      <td style="padding:20px 24px 0 24px;">
        <div style="font-size:14.5px;color:#333;line-height:1.65;">
          {_html_escape(reason)}</div>
      </td>
    </tr>

    <!-- Metric cards -->
    <tr>
      <td style="padding:16px 24px;">
        <table cellpadding="0" cellspacing="0" border="0" width="100%">
          <tr>
            <td style="padding:10px 12px;background:#f8f9fa;border-radius:6px;
              width:48%;vertical-align:top;">
              <div style="font-size:10px;color:#999;text-transform:uppercase;
                letter-spacing:0.6px;margin-bottom:3px;">Catalyst Quality</div>
              <div style="font-size:13.5px;font-weight:600;color:#333;">
                {_html_escape(cat_quality)}</div>
            </td>
            <td style="width:4%;">&nbsp;</td>
            <td style="padding:10px 12px;background:#f8f9fa;border-radius:6px;
              width:48%;vertical-align:top;">
              <div style="font-size:10px;color:#999;text-transform:uppercase;
                letter-spacing:0.6px;margin-bottom:3px;">Float Assessment</div>
              <div style="font-size:13.5px;font-weight:600;color:#333;">
                {_html_escape(float_label)}</div>
            </td>
          </tr>
        </table>
      </td>
    </tr>

    <!-- Key risk -->
    <tr>
      <td style="padding:0 24px 16px 24px;">
        <div style="font-size:10px;color:#999;text-transform:uppercase;
          letter-spacing:0.6px;margin-bottom:3px;">Key Risk</div>
        <div style="font-size:13px;color:{style['color']};font-weight:600;
          line-height:1.45;">{_html_escape(key_risk)}</div>
      </td>
    </tr>

    <!-- Red flags -->
    <tr>
      <td style="padding:0 24px 16px 24px;">
        <div style="font-size:11px;font-weight:700;color:#555;
          text-transform:uppercase;letter-spacing:0.6px;margin-bottom:8px;
          border-bottom:2px solid {style['color']};padding-bottom:4px;">
          ⚑ Red Flags ({len(red_flags)})</div>
        <table cellpadding="0" cellspacing="0" border="0" width="100%">
          {flags_html}
        </table>
      </td>
    </tr>

    <!-- Footer -->
    <tr>
      <td style="border-top:1px solid #e0e0e0;padding:14px 24px;">
        <div style="font-size:10.5px;color:#aaa;line-height:1.55;">
          \U0001f916 Automated by&nbsp;<strong>Jerry Trader</strong>
          SignalEngine&nbsp;+&nbsp;Claude Code&nbsp;headless<br>
          <span style="color:#ccc;">Re: {_html_escape(catalyst_subject)}</span>
        </div>
      </td>
    </tr>
  </table>

</td></tr>
</table>
</body>
</html>"""

    def _send_diagnosis_email(
        self,
        catalyst_subject: str,
        diagnosis: dict[str, Any],
        in_reply_to: str | None = None,
    ) -> None:
        """Compose and send a threaded HTML diagnosis follow-up email."""
        if not self._smtp:
            return

        try:
            from_addr = os.getenv(self._smtp.username_env, "")
            password = os.getenv(self._smtp.password_env, "")

            if not from_addr or not password:
                logger.error("Diagnosis email: missing SMTP credentials")
                return

            verdict_key = diagnosis.get("verdict", "avoid").lower()
            style = self._VERDICT_STYLE.get(verdict_key, self._VERDICT_STYLE["avoid"])
            score = diagnosis.get("tradability_score", "?")
            # "Re:" prefix keeps Gmail threading; verdict tag gives at-a-glance info
            new_subj = (
                f"Re: [{style['label']} {score}/5] {catalyst_subject}"
            )

            # ── Plain-text fallback ──────────────────────────────────────
            lines: list[str] = [
                f"Verdict: {style['label']}",
                f"Score: {score}/5",
                f"Reason: {diagnosis.get('verdict_reason', 'N/A')}",
                f"Catalyst quality: {diagnosis.get('catalyst_quality', 'N/A')}",
                f"Float assessment: {diagnosis.get('float_assessment', 'N/A')}",
                f"Key risk: {diagnosis.get('key_risk', 'N/A')}",
                "",
                "Red flags:",
            ]
            red_flags: list[str] = diagnosis.get("red_flags", [])
            for flag in red_flags:
                lines.append(f"  • {flag}")
            if not red_flags:
                lines.append("  (none)")
            lines += [
                "",
                "---",
                f"Original: {catalyst_subject}",
                "Automated by Jerry Trader SignalEngine + Claude Code headless",
            ]
            text_body = "\n".join(lines)

            # ── HTML body ────────────────────────────────────────────────
            html_body = self._build_diagnosis_html(
                verdict_key, diagnosis, catalyst_subject
            )

            # ── Compose multipart ────────────────────────────────────────
            msg = EmailMessage()
            msg["Subject"] = new_subj
            msg["From"] = from_addr
            msg["To"] = from_addr

            if in_reply_to:
                msg["In-Reply-To"] = in_reply_to
                msg["References"] = in_reply_to

            msg.set_content(text_body)
            msg.add_alternative(html_body, subtype="html")

            with smtplib.SMTP(self._smtp.host, self._smtp.port, timeout=10) as smtp:
                smtp.starttls()
                smtp.login(from_addr, password)
                smtp.send_message(msg)

            sep = "=" * 50
            logger.info(
                f"\n{sep}\n"
                f"Diagnosis email sent: {style['label']} (score={score}/5)\n"
                f"In-Reply-To: {in_reply_to or '(none)'}\n"
                f"Subject: {new_subj}\n"
                f"{text_body}\n"
                f"{sep}"
            )

        except Exception as exc:
            logger.error(f"Diagnosis email send failed: {exc}")

    # ── Email composition & delivery ────────────────────────────────────────

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
    ) -> tuple[bool, str | None]:
        """Send the email. Returns ``(success, message_id)``.

        The *message_id* is used by the diagnosis follow-up to set
        ``In-Reply-To`` / ``References`` so Gmail threads them together.
        """
        if not self._smtp:
            return False, None

        try:
            from_addr = os.getenv(self._smtp.username_env, "")
            password = os.getenv(self._smtp.password_env, "")

            if not from_addr or not password:
                logger.error(
                    f"Notification: missing SMTP credentials "
                    f"({self._smtp.username_env}, {self._smtp.password_env})"
                )
                return False, None

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

            message_id = msg["Message-ID"] or None

            # Log the full email content as one block (like NewsProcessor)
            sep = "=" * 50
            logger.info(
                f"\n{sep}\n"
                f"Notification sent: {event_name} / {symbol}\n"
                f"Message-ID: {message_id}\n"
                f"Subject: {subject}\n"
                f"{body}"
                f"{sep}"
            )
            return True, message_id

        except Exception as e:
            logger.error(
                f"Notification send failed: event={event_name} symbol={symbol} - {e}"
            )
            return False, None

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
            # Defaults for optional fields — overridden if static / market data available
            "change_pct": "N/A",
            "volume": "N/A",
            "float": "N/A",
            "marketCap": "N/A",
            "sector": "N/A",
            "borrow_fee": "N/A",
            "available_shares": "N/A",
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
        for key in ("float", "marketCap", "volume", "available_shares"):
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

        # Squeeze context: when shares are exhausted and fee is high
        avail = vars.get("available_shares", "N/A")
        fee = vars.get("borrow_fee", "N/A")
        if avail == "0" and fee != "N/A":
            vars["squeeze_note"] = (
                f"⚡ Short-squeeze conditions: 0 shares available to borrow "
                f"@ {fee}. With positive catalyst, this is a classic squeeze setup."
            )
        elif avail == "0":
            vars["squeeze_note"] = (
                "⚡ No shares available to borrow — potential squeeze fuel "
                "if catalyst triggers buying pressure."
            )
        else:
            vars["squeeze_note"] = ""

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
