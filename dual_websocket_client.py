"""Dual WebSocket client for Capital.com and EODHD.

This module is intentionally written as a single executable script so that it can
be deployed on Windows environments without additional packaging steps.  It
relies on asyncio in order to run the two streaming clients concurrently while
sharing a single PostgreSQL connection pool per provider.

Key features implemented as per requirements:

* Credentials and configuration are loaded from a local `.env` file.
* Robust logging with timestamps is provided via the standard ``logging``
  module.
* Automatic reconnection is performed whenever a stream goes silent for longer
  than the configured thresholds (30 seconds during market hours, 10 minutes
  otherwise) or when network exceptions arise.
* Both clients validate the connection prior to dispatching subscription
  messages and guard all network operations with defensive exception handling
  so that the process can run unattended for extended periods of time.
* Streaming payloads are validated before being persisted into PostgreSQL using
  ``asyncpg``.

The file can be executed directly with ``python dual_websocket_client.py``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timezone
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Tuple

import aiohttp
import asyncpg
import websockets
from dotenv import load_dotenv
from websockets.legacy.client import WebSocketClientProtocol


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------


def configure_logging() -> None:
    """Configure application wide logging settings."""

    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


configure_logging()
LOGGER = logging.getLogger("dual_ws_client")


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SymbolMapping:
    """Mapping between Capital.com epics, EODHD tickers and asset IDs."""

    capital_epic: str
    eodhd_ticker: str
    asset_id: int


def parse_symbol_mappings(raw: str) -> List[SymbolMapping]:
    """Parse the SYMBOLS environment variable into structured mappings."""

    mappings: List[SymbolMapping] = []
    for entry in (raw or "").split(","):
        entry = entry.strip()
        if not entry:
            continue
        try:
            epic, ticker, asset_id_str = entry.split(":")
            asset_id = int(asset_id_str)
        except ValueError as exc:  # pragma: no cover - guardrail
            LOGGER.error("Invalid SYMBOLS entry '%s': %s", entry, exc)
            continue
        mappings.append(SymbolMapping(capital_epic=epic, eodhd_ticker=ticker, asset_id=asset_id))
    return mappings


def env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def is_market_hours(now: Optional[datetime] = None) -> bool:
    """Return True when the current UTC time is inside regular US market hours."""

    now = now or utc_now()
    # US equities market hours (09:30-16:00 ET) converted to UTC.
    # 09:30 ET -> 14:30 UTC (during standard time) / 13:30 UTC (during DST).
    # To avoid timezone pitfalls we consider 13:30-20:00 UTC as an inclusive
    # window, which safely covers both possibilities and errs on the side of a
    # shorter inactivity threshold when markets might be active.
    start = dt_time(hour=13, minute=30)
    end = dt_time(hour=20, minute=15)
    return start <= now.time() <= end and now.weekday() < 5


# ---------------------------------------------------------------------------
# Database management
# ---------------------------------------------------------------------------


class DatabaseManager:
    """Manages PostgreSQL connection pools for the different data sinks."""

    def __init__(self, capital_cfg: Dict[str, Any], eod_cfg: Dict[str, Any]):
        self._capital_cfg = capital_cfg
        self._eod_cfg = eod_cfg
        self._capital_pool: Optional[asyncpg.Pool] = None
        self._eod_pool: Optional[asyncpg.Pool] = None

    async def start(self) -> None:
        LOGGER.info("Creating PostgreSQL connection pools")
        self._capital_pool = await asyncpg.create_pool(**self._capital_cfg, command_timeout=60)
        self._eod_pool = await asyncpg.create_pool(**self._eod_cfg, command_timeout=60)

    async def close(self) -> None:
        if self._capital_pool:
            await self._capital_pool.close()
        if self._eod_pool:
            await self._eod_pool.close()

    @property
    def capital_pool(self) -> asyncpg.Pool:
        if not self._capital_pool:
            raise RuntimeError("Capital.com pool not initialised")
        return self._capital_pool

    @property
    def eod_pool(self) -> asyncpg.Pool:
        if not self._eod_pool:
            raise RuntimeError("EODHD pool not initialised")
        return self._eod_pool


# ---------------------------------------------------------------------------
# Base WebSocket client
# ---------------------------------------------------------------------------


class ReconnectingWebSocketClient:
    """Base class implementing reconnection logic and inactivity monitoring."""

    INACTIVITY_MARKET_SECONDS = 30
    INACTIVITY_OFF_MARKET_SECONDS = 600  # 10 minutes

    def __init__(self, name: str) -> None:
        self.name = name
        self._watchdog_task: Optional[asyncio.Task[None]] = None
        self._ws: Optional[WebSocketClientProtocol] = None
        self._last_message_ts: float = time.monotonic()
        self._stop = asyncio.Event()
        self._subscription_sent = False

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - runtime safeguard
                LOGGER.exception("%s encountered an error: %s", self.name, exc)
            backoff = 5
            LOGGER.info("%s reconnecting in %s seconds", self.name, backoff)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                continue

    async def stop(self) -> None:
        self._stop.set()
        if websocket_is_open(self._ws):
            await close_websocket(self._ws, code=1000, reason="shutdown")
        if self._watchdog_task:
            self._watchdog_task.cancel()

    async def _connect_and_listen(self) -> None:
        self._subscription_sent = False
        async with self._establish_connection() as ws:
            self._ws = ws
            self._last_message_ts = time.monotonic()
            LOGGER.info("%s connection established", self.name)

            await self._send_subscription(ws)
            self._subscription_sent = True
            LOGGER.info("%s subscription sent", self.name)

            self._watchdog_task = asyncio.create_task(self._watchdog(ws), name=f"{self.name}-watchdog")

            try:
                async for raw_message in ws:
                    self._last_message_ts = time.monotonic()
                    try:
                        await self._handle_message(raw_message)
                    except Exception as exc:  # pragma: no cover - defensive path
                        LOGGER.exception("%s failed to process message: %s", self.name, exc)
            except websockets.ConnectionClosed as exc:
                LOGGER.warning("%s connection closed: %s", self.name, exc)
            finally:
                if self._watchdog_task:
                    self._watchdog_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await self._watchdog_task
                LOGGER.info("%s listener loop ended", self.name)

    async def _watchdog(self, ws: WebSocketClientProtocol) -> None:
        try:
            while True:
                timeout = (
                    self.INACTIVITY_MARKET_SECONDS
                    if is_market_hours()
                    else self.INACTIVITY_OFF_MARKET_SECONDS
                )
                await asyncio.sleep(5)
                elapsed = time.monotonic() - self._last_message_ts
                if elapsed > timeout:
                    LOGGER.warning(
                        "%s no data for %.1f seconds (timeout=%s) -> reconnect",
                        self.name,
                        elapsed,
                        timeout,
                    )
                    try:
                        await close_websocket(ws, code=4000, reason="inactivity")
                    finally:
                        return
        except asyncio.CancelledError:
            return

    # --- Hooks for subclasses -------------------------------------------------

    @asynccontextmanager
    async def _establish_connection(self) -> AsyncIterator[WebSocketClientProtocol]:
        ws = await self._connect()
        try:
            yield ws
        finally:
            if websocket_is_open(ws):
                await close_websocket(ws)

    async def _connect(self) -> WebSocketClientProtocol:
        raise NotImplementedError

    async def _send_subscription(self, ws: WebSocketClientProtocol) -> None:
        raise NotImplementedError

    async def _handle_message(self, raw_message: str) -> None:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Capital.com client
# ---------------------------------------------------------------------------


class CapitalComClient(ReconnectingWebSocketClient):

    DEFAULT_REST_URL = "https://api-capital.backend-capital.com/api"
    DEFAULT_STREAMING_URL = "wss://api-streaming-capital.com/connect"
    DEMO_REST_URL = "https://demo-api-capital.com/api"
    DEMO_STREAMING_URL = "wss://demo-streaming-capital.com/connect"

    def __init__(
        self,
        mappings: Iterable[SymbolMapping],
        api_key: str,
        email: str,
        password: str,
        db_pool: asyncpg.Pool,
        rest_url: Optional[str] = None,
        streaming_url: Optional[str] = None,
        *,
        allow_demo_fallback: bool = False,
    ) -> None:
        super().__init__(name="Capital.com")
        self._mappings = list(mappings)
        self._api_key = api_key
        self._email = email
        self._password = password
        self._db_pool = db_pool
        self._cst: Optional[str] = None
        self._security_token: Optional[str] = None
        self._http_session: Optional[aiohttp.ClientSession] = None
        self._auth_lock = asyncio.Lock()
        self._token_expiry: float = 0.0
        self._rest_url = (rest_url or self.DEFAULT_REST_URL).rstrip("/")
        self._streaming_url = (streaming_url or self.DEFAULT_STREAMING_URL).rstrip("/")
        self._allow_demo_fallback = allow_demo_fallback and rest_url is None and streaming_url is None
        if self._allow_demo_fallback:
            LOGGER.info("Capital.com demo fallback enabled via configuration")

    async def stop(self) -> None:
        await super().stop()
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()

    async def _connect(self) -> WebSocketClientProtocol:
        await self._authenticate()
        headers = {
            "CST": self._cst or "",
            "X-SECURITY-TOKEN": self._security_token or "",
        }
        LOGGER.debug("Capital.com connecting with headers: %s", headers.keys())
        return await websockets.connect(
            self._streaming_url,
            extra_headers=headers,
            ping_interval=20,
            ping_timeout=20,
        )

    async def _authenticate(self) -> None:
        async with self._auth_lock:
            if self._tokens_are_valid():
                return

            if self._http_session is None or self._http_session.closed:
                self._http_session = aiohttp.ClientSession()

            attempt_urls = [(self._rest_url, self._streaming_url)]
            if self._allow_demo_fallback and self._rest_url == self.DEFAULT_REST_URL:
                attempt_urls.append((self.DEMO_REST_URL, self.DEMO_STREAMING_URL))

            last_error: Optional[str] = None
            for idx, (rest_url, streaming_url) in enumerate(attempt_urls):
                if idx > 0:
                    LOGGER.warning(
                        "Capital.com primary authentication failed; retrying against demo endpoints"
                    )
                auth_url = f"{rest_url}/v1/session"
                payload = {"identifier": self._email, "password": self._password}
                header_variants = ("X-CAP-API-KEY", "X-IG-API-KEY")

                for header_name in header_variants:
                    headers = {
                        header_name: self._api_key,
                        "Content-Type": "application/json",
                        "Accept": "application/json; charset=UTF-8",
                        "Version": "1",
                        "Referer": "https://capital.com/",
                    }

                    try:
                        async with self._http_session.post(
                            auth_url,
                            headers=headers,
                            json=payload,
                            timeout=30,
                        ) as resp:
                            text_body = await resp.text()
                            if resp.status != 200:
                                last_error = (
                                    f"Capital.com authentication failed ({resp.status}) using {header_name}:"
                                    f" {text_body.strip()}"
                                )
                                LOGGER.error("%s", last_error)
                                if resp.status == 405:
                                    LOGGER.warning(
                                        "Capital.com login rejected with 405 at %s using %s; verify environment",
                                        rest_url,
                                        header_name,
                                    )
                                continue
                            try:
                                data = json.loads(text_body) if text_body else {}
                            except json.JSONDecodeError:
                                data = {}

                            self._cst = resp.headers.get("CST") or data.get("CST")
                            self._security_token = resp.headers.get("X-SECURITY-TOKEN") or data.get(
                                "securityToken"
                            )
                            if not self._cst or not self._security_token:
                                last_error = (
                                    "Capital.com authentication missing CST or X-SECURITY-TOKEN from response"
                                )
                                LOGGER.error("%s", last_error)
                                self._cst = None
                                self._security_token = None
                                continue
                            self._rest_url = rest_url.rstrip("/")
                            self._streaming_url = streaming_url.rstrip("/")
                            self._update_token_expiry(data)
                            LOGGER.info(
                                "Capital.com authenticated against %s using %s header",
                                rest_url,
                                header_name,
                            )
                            break
                    except aiohttp.ClientError as exc:
                        last_error = f"Capital.com authentication network error: {exc}"
                        LOGGER.error("%s", last_error)
                        continue
                else:
                    # try next rest url
                    continue
                break

            if not self._tokens_are_valid():
                raise RuntimeError(last_error or "Capital.com authentication failed")

    async def _send_subscription(self, ws: WebSocketClientProtocol) -> None:
        if not websocket_is_open(ws):
            raise RuntimeError("Capital.com WebSocket not open during subscription")
        for mapping in self._mappings:
            subscribe_message = {
                "destination": f"market:{mapping.capital_epic}",
                "command": "subscribe",
            }
            await ws.send(json.dumps(subscribe_message))
            LOGGER.info("Capital.com subscribed to %s", mapping.capital_epic)

    async def _handle_message(self, raw_message: str) -> None:
        message = json.loads(raw_message)

        if "heartbeat" in message:
            LOGGER.debug("Capital.com heartbeat: %s", message)
            return

        body = message.get("body") or {}
        if not body:
            LOGGER.debug("Capital.com empty body: %s", message)
            return

        epic = body.get("epic") or body.get("instrumentName")
        if not epic:
            LOGGER.warning("Capital.com message missing epic: %s", message)
            return

        mapping = next((m for m in self._mappings if m.capital_epic == epic), None)
        if not mapping:
            LOGGER.debug("Capital.com received unknown epic %s", epic)
            return

        msg_type = body.get("type") or message.get("type")
        epoch = body.get("timestamp") or body.get("epoch")
        if epoch is None:
            LOGGER.debug("Capital.com message missing epoch: %s", message)
            return

        try:
            event_time = datetime.fromtimestamp(int(epoch) / 1000, tz=timezone.utc)
        except Exception:  # pragma: no cover - defensive parsing
            event_time = utc_now()

        record = {
            "asset_id": mapping.asset_id,
            "timestamp": event_time,
            "symbol": epic,
            "tipo": msg_type or "unknown",
            "precio_compra": _maybe_float(body.get("bid")),
            "cantidad_disponible_compra": _maybe_float(body.get("bidSize")),
            "precio_venta": _maybe_float(body.get("offer")),
            "cantidad_disponible_venta": _maybe_float(body.get("offerSize")),
            "epoch": int(epoch),
        }

        if record["precio_compra"] is None and record["precio_venta"] is None:
            LOGGER.debug("Capital.com skipping record without price: %s", message)
            return

        await self._insert_record(record)

    async def _insert_record(self, record: Dict[str, Any]) -> None:
        query = (
            "INSERT INTO real_time_cfd (asset_id, timestamp, symbol, tipo, precio_compra, "
            "cantidad_disponible_compra, precio_venta, cantidad_disponible_venta, epoch) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
        )
        try:
            await self._db_pool.execute(
                query,
                record["asset_id"],
                record["timestamp"],
                record["symbol"],
                record["tipo"],
                record["precio_compra"],
                record["cantidad_disponible_compra"],
                record["precio_venta"],
                record["cantidad_disponible_venta"],
                record["epoch"],
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.exception("Capital.com failed to insert record: %s", exc)

    def _tokens_are_valid(self) -> bool:
        if not self._cst or not self._security_token:
            return False
        if self._token_expiry <= 0:
            return False
        return time.monotonic() < self._token_expiry

    def _update_token_expiry(self, payload: Dict[str, Any]) -> None:
        expiry_seconds: Optional[float] = None
        expiry_epoch = payload.get("expiryTime") or payload.get("timeUntilExpiry")
        if isinstance(expiry_epoch, (int, float)):
            now_epoch = time.time()
            if expiry_epoch > 1e12:  # milliseconds
                expiry_seconds = (expiry_epoch / 1000) - now_epoch
            elif expiry_epoch > 1e9:  # seconds timestamp
                expiry_seconds = expiry_epoch - now_epoch
            else:
                expiry_seconds = float(expiry_epoch)
        elif isinstance(expiry_epoch, str):
            try:
                expiry_seconds = float(expiry_epoch)
            except ValueError:
                expiry_seconds = None

        if not expiry_seconds or expiry_seconds <= 0:
            expiry_seconds = 55 * 60  # default to 55 minutes

        self._token_expiry = time.monotonic() + expiry_seconds
        ttl = max(0.0, self._token_expiry - time.monotonic())
        LOGGER.debug("Capital.com session token valid for %.0f seconds", ttl)


# ---------------------------------------------------------------------------
# EODHD client
# ---------------------------------------------------------------------------


class EODHDClient(ReconnectingWebSocketClient):
    BASE_WS_URL = "wss://ws.eodhistoricaldata.com/ws/us/"

    def __init__(
        self,
        mappings: Iterable[SymbolMapping],
        api_token: str,
        db_pool: asyncpg.Pool,
    ) -> None:
        super().__init__(name="EODHD")
        self._mappings = list(mappings)
        self._api_token = api_token
        self._db_pool = db_pool

    async def _connect(self) -> WebSocketClientProtocol:
        url = f"{self.BASE_WS_URL}?api_token={self._api_token}&s=1"
        LOGGER.debug("EODHD connecting to %s", url)
        return await websockets.connect(url, ping_interval=20, ping_timeout=20)

    async def _send_subscription(self, ws: WebSocketClientProtocol) -> None:
        if not websocket_is_open(ws):
            raise RuntimeError("EODHD WebSocket not open during subscription")

        symbols = ",".join(m.eodhd_ticker for m in self._mappings)
        payload = {"action": "subscribe", "symbols": symbols}
        await ws.send(json.dumps(payload))
        LOGGER.info("EODHD subscribed to %s", symbols)

    async def _handle_message(self, raw_message: str) -> None:
        message = json.loads(raw_message)

        msg_type = message.get("type") or message.get("event")
        if not msg_type:
            LOGGER.debug("EODHD untyped message: %s", message)
            return

        symbol = message.get("s") or message.get("symbol")
        if not symbol:
            LOGGER.debug("EODHD missing symbol: %s", message)
            return

        mapping = next((m for m in self._mappings if m.eodhd_ticker == symbol), None)
        if not mapping:
            LOGGER.debug("EODHD received unknown symbol %s", symbol)
            return

        event_ts = message.get("t") or message.get("timestamp")
        try:
            if isinstance(event_ts, (int, float)):
                event_time = datetime.fromtimestamp(float(event_ts), tz=timezone.utc)
            elif isinstance(event_ts, str) and event_ts.isdigit():
                event_time = datetime.fromtimestamp(float(event_ts), tz=timezone.utc)
            else:
                event_time = utc_now()
        except Exception:  # pragma: no cover - fallback to now
            event_time = utc_now()

        if msg_type.lower() == "trade":
            await self._handle_trade(mapping, symbol, event_time, message)
        elif msg_type.lower() in {"quote", "book"}:
            await self._handle_quote(mapping, symbol, event_time, message)
        else:
            LOGGER.debug("EODHD ignoring type %s", msg_type)

    async def _handle_trade(
        self,
        mapping: SymbolMapping,
        symbol: str,
        event_time: datetime,
        message: Dict[str, Any],
    ) -> None:
        price = _maybe_float(message.get("p") or message.get("price"))
        size = _maybe_float(message.get("v") or message.get("size"))
        if price is None or size is None:
            LOGGER.debug("EODHD trade missing price/size: %s", message)
            return

        query = (
            "INSERT INTO trades_real_time (asset_id, symbol, session, price, size, condition_code, "
            "dark_pool, market_status, event_timestamp) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)"
        )

        await self._db_pool.execute(
            query,
            mapping.asset_id,
            symbol,
            message.get("session"),
            price,
            size,
            message.get("condition_code"),
            message.get("dark_pool"),
            message.get("market_status"),
            event_time,
        )

    async def _handle_quote(
        self,
        mapping: SymbolMapping,
        symbol: str,
        event_time: datetime,
        message: Dict[str, Any],
    ) -> None:
        bid_price = _maybe_float(message.get("bid") or message.get("bid_price"))
        ask_price = _maybe_float(message.get("ask") or message.get("ask_price"))
        bid_size = _maybe_float(message.get("bid_size") or message.get("bidSize"))
        ask_size = _maybe_float(message.get("ask_size") or message.get("askSize"))

        if bid_price is None and ask_price is None:
            LOGGER.debug("EODHD quote missing both bid/ask: %s", message)
            return

        query = (
            "INSERT INTO quotes_real_time (asset_id, symbol, session, bid_price, ask_price, bid_size, "
            "ask_size, event_timestamp) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)"
        )

        await self._db_pool.execute(
            query,
            mapping.asset_id,
            symbol,
            message.get("session"),
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            event_time,
        )


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _maybe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def websocket_is_open(ws: Optional[Any]) -> bool:
    """Best-effort check that works across websockets versions."""

    if ws is None:
        return False

    closed_attr = getattr(ws, "closed", None)
    if isinstance(closed_attr, bool):
        return not closed_attr
    if callable(closed_attr):
        try:
            result = closed_attr()
        except TypeError:
            result = closed_attr
        if isinstance(result, bool):
            return not result
        if hasattr(result, "done") and callable(result.done):  # asyncio Future
            return not result.done()

    open_attr = getattr(ws, "open", None)
    if isinstance(open_attr, bool):
        return open_attr
    if callable(open_attr):
        try:
            result = open_attr()
        except TypeError:
            result = open_attr
        if isinstance(result, bool):
            return result

    state_attr = getattr(ws, "state", None)
    if state_attr is not None:
        state_name = getattr(state_attr, "name", str(state_attr)).lower()
        if "open" in state_name:
            return True
        if "closed" in state_name:
            return False

    ready_state = getattr(ws, "readyState", None)
    if ready_state is not None:
        try:
            return int(ready_state) == 1
        except (TypeError, ValueError):
            pass

    return True


async def close_websocket(ws: Optional[Any], *, code: int = 1000, reason: str = "") -> None:
    if ws is None:
        return

    close_callable = getattr(ws, "close", None)
    if close_callable is None:
        return

    try:
        result = close_callable(code=code, reason=reason)
    except TypeError:
        result = close_callable()

    if asyncio.iscoroutine(result) or isinstance(result, asyncio.Future):
        await result


@asynccontextmanager
async def graceful_shutdown(clients: Iterable[ReconnectingWebSocketClient], db: DatabaseManager):
    try:
        yield
    finally:
        LOGGER.info("Stopping clients and closing database connections")
        await asyncio.gather(*(client.stop() for client in clients), return_exceptions=True)
        await db.close()


def read_env() -> Tuple[
    List[SymbolMapping],
    Dict[str, Any],
    Dict[str, Any],
    Dict[str, str],
    Dict[str, Optional[str]],
]:
    load_dotenv()

    symbol_mappings = parse_symbol_mappings(os.environ.get("SYMBOLS", ""))
    if not symbol_mappings:
        raise RuntimeError("No valid SYMBOLS mapping found in environment")

    capital_cfg = {
        "host": os.environ.get("PGHOST"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "database": os.environ.get("PGDATABASE"),
        "user": os.environ.get("PGUSER"),
        "password": os.environ.get("PGPASSWORD"),
    }

    eod_cfg = {
        "host": os.environ.get("DB_HOST"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "database": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
    }

    for name, cfg in {"Capital.com DB": capital_cfg, "EODHD DB": eod_cfg}.items():
        missing = [key for key, value in cfg.items() if value in (None, "")]
        if missing:
            raise RuntimeError(f"Missing database configuration for {name}: {missing}")

    provider_credentials = {
        "capital_api_key": os.environ.get("CAPITAL_API_KEY", ""),
        "capital_email": os.environ.get("CAPITAL_EMAIL", ""),
        "capital_password": os.environ.get("CAPITAL_PASSWORD", ""),
        "eod_api_token": os.environ.get("API_TOKEN", ""),
    }

    for key, value in provider_credentials.items():
        if not value:
            raise RuntimeError(f"Missing required credential: {key}")

    capital_urls = {
        "rest": os.environ.get("CAPITAL_REST_BASE"),
        "streaming": os.environ.get("CAPITAL_STREAMING_URL"),
        "allow_demo_fallback": env_flag("CAPITAL_ENABLE_DEMO_FALLBACK", False),
    }

    return symbol_mappings, capital_cfg, eod_cfg, provider_credentials, capital_urls


async def main() -> None:
    symbol_mappings, capital_db_cfg, eod_db_cfg, credentials, capital_urls = read_env()

    db_manager = DatabaseManager(capital_db_cfg, eod_db_cfg)
    await db_manager.start()

    capital_client = CapitalComClient(
        mappings=symbol_mappings,
        api_key=credentials["capital_api_key"],
        email=credentials["capital_email"],
        password=credentials["capital_password"],
        db_pool=db_manager.capital_pool,
        rest_url=capital_urls.get("rest"),
        streaming_url=capital_urls.get("streaming"),
        allow_demo_fallback=capital_urls.get("allow_demo_fallback", False),
    )

    eod_client = EODHDClient(
        mappings=symbol_mappings,
        api_token=credentials["eod_api_token"],
        db_pool=db_manager.eod_pool,
    )

    clients: Tuple[ReconnectingWebSocketClient, ...] = (capital_client, eod_client)

    loop = asyncio.get_running_loop()

    stop_event = asyncio.Event()

    def _signal_handler(sig: int) -> None:
        LOGGER.info("Received signal %s, initiating shutdown", sig)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler, sig)
        except NotImplementedError:  # Windows compatibility (SIGTERM missing)
            signal.signal(sig, lambda s, f: _signal_handler(sig))

    async with graceful_shutdown(clients, db_manager):
        tasks = [asyncio.create_task(client.run(), name=f"{client.name}-runner") for client in clients]

        await stop_event.wait()
        LOGGER.info("Cancellation requested, waiting for clients to stop")
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOGGER.info("Interrupted by user")
    except Exception as exc:  # pragma: no cover - entrypoint guard
        LOGGER.exception("Fatal error: %s", exc)
        sys.exit(1)
