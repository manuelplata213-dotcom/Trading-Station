import asyncio
import json
import logging
import time
from collections import deque
from decimal import Decimal
from enum import Enum
from typing import Any, Deque, Dict, Optional

from config import settings
from exchange_client import ExchangeClient
from strategy_engine import QuantitativeStrategyEngine

logger = logging.getLogger("quant.execution")


class EngineState(str, Enum):
    IDLE = "IDLE"
    EVALUATING = "EVALUATING"
    ORDER_SUBMITTED = "ORDER_SUBMITTED"
    POSITION_OPEN = "POSITION_OPEN"
    TRAILING_EXIT = "TRAILING_EXIT"
    HALTED = "HALTED"


class ExecutionManager:
    """
    Deterministic Finite-State Machine managing capital allocation, order verification,
    circuit breakers, and real-time telemetry state.
    """

    def __init__(self, exchange: ExchangeClient, strategy: QuantitativeStrategyEngine) -> None:
        self.exchange = exchange
        self.strategy = strategy

        self.state: EngineState = EngineState.IDLE
        self.running: bool = False
        self._lock = asyncio.Lock()

        # Position & Accounting State
        self.position_qty: Decimal = Decimal("0")
        self.avg_entry_price: Decimal = Decimal("0")
        self.highest_since_entry: Decimal = Decimal("0")
        self.target_exit_price: Decimal = Decimal("0")
        self.trailing_stop_price: Decimal = Decimal("0")
        self.active_order_id: Optional[str] = None

        # Risk & PnL Telemetry
        self.realized_pnl_usd: Decimal = Decimal("0")
        self.daily_pnl_usd: Decimal = Decimal("0")
        self.peak_equity_usd: Decimal = Decimal("0")
        self.total_trades: int = 0
        self.winning_trades: int = 0
        self.last_price: Decimal = Decimal("0")
        self.last_reason: str = "System initialized in standby"

        # Rolling Telemetry History
        self.pnl_history: Deque[Dict[str, Any]] = deque(maxlen=200)
        self.trade_ledger: Deque[Dict[str, Any]] = deque(maxlen=100)
        self.order_book: Dict[str, Any] = {"bids": [], "asks": []}

        # Optional Redis / Postgres Handles
        self._redis = None
        self._db_pool = None

    async def init_persistence(self) -> None:
        if settings.REDIS_URL:
            try:
                import redis.asyncio as aioredis
                self._redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
                await self._redis.ping()
                logger.info(json.dumps({"event": "redis_connected"}))
            except Exception as exc:
                logger.warning(json.dumps({"event": "redis_fallback_memory", "error": str(exc)}))
                self._redis = None

        if settings.DATABASE_URL:
            try:
                import asyncpg
                self._db_pool = await asyncpg.create_pool(settings.DATABASE_URL, min_size=1, max_size=5)
                async with self._db_pool.acquire() as conn:
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS trade_ledger (
                            id SERIAL PRIMARY KEY,
                            ts TIMESTAMPTZ DEFAULT NOW(),
                            symbol TEXT,
                            side TEXT,
                            qty NUMERIC,
                            price NUMERIC,
                            pnl_usd NUMERIC,
                            order_id TEXT
                        );
                    """)
                logger.info(json.dumps({"event": "timescaledb_connected"}))
            except Exception as exc:
                logger.warning(json.dumps({"event": "db_fallback_memory", "error": str(exc)}))
                self._db_pool = None
    def start_engine(self) -> None:
        if self.state == EngineState.HALTED:
            self.state = EngineState.IDLE
        self.running = True
        self.last_reason = "Autonomous execution loop activated"
        logger.info(json.dumps({"event": "engine_started"}))

    def pause_engine(self) -> None:
        self.running = False
        self.last_reason = "Execution loop paused by operator"
        logger.info(json.dumps({"event": "engine_paused"}))

    def _calculate_order_size_usd(self, available_quote: Decimal) -> Decimal:
        if settings.ALLOCATION_MODE == "PERCENT":
            target = available_quote * settings.EQUITY_ALLOCATION_PCT
        else:
            target = settings.FIXED_CAPITAL_USD

        capped = min(target, available_quote, settings.MAX_CONCURRENT_EXPOSURE_USD)
        return max(Decimal("0"), capped)

    def _check_circuit_breakers(self, current_equity: Decimal) -> Optional[str]:
        if self.peak_equity_usd == Decimal("0"):
            self.peak_equity_usd = current_equity
        elif current_equity > self.peak_equity_usd:
            self.peak_equity_usd = current_equity

        if self.daily_pnl_usd <= -abs(settings.MAX_DAILY_LOSS_USD):
            return f"Daily loss limit breached (${self.daily_pnl_usd:.2f})"

        if self.peak_equity_usd > Decimal("0"):
            drawdown = (self.peak_equity_usd - current_equity) / self.peak_equity_usd
            if drawdown >= settings.MAX_DRAWDOWN_PCT:
                return f"Max drawdown limit breached ({drawdown * 100:.2f}%)"

        return None

    async def _reconcile_order(self, order_id: str, timeout_sec: float = 10.0) -> Dict[str, Any]:
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            status = await self.exchange.fetch_order_status(order_id)
            if status.get("status") in ("closed", "filled"):
                return status
            if status.get("status") in ("canceled", "rejected", "expired"):
                raise RuntimeError(f"Order {order_id} terminated with status {status.get('status')}")
            await asyncio.sleep(0.4)
        raise TimeoutError(f"Order {order_id} fill reconciliation timed out")

    async def _record_trade(self, side: str, qty: Decimal, price: Decimal, pnl: Decimal, order_id: str) -> None:
        entry = {
            "timestamp": int(time.time()),
            "symbol": settings.SYMBOL,
            "side": side.upper(),
            "qty": float(qty),
            "price": float(price),
            "pnl_usd": round(float(pnl), 4),
            "order_id": order_id,
        }
        self.trade_ledger.appendleft(entry)
        logger.info(json.dumps({"event": "trade_filled", **entry}))

        if self._db_pool:
            async with self._db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO trade_ledger(symbol, side, qty, price, pnl_usd, order_id) VALUES($1,$2,$3,$4,$5,$6)",
                    settings.SYMBOL, side.upper(), qty, price, pnl, order_id
                )
    async def step(self) -> None:
        """
        Single deterministic tick cycle executed by background worker.
        """
        async with self._lock:
            ticker = await self.exchange.fetch_ticker_snapshot()
            self.last_price = ticker["last"]
            self.strategy.ingest_tick(self.last_price)

            try:
                self.order_book = await self.exchange.fetch_order_book_depth(limit=6)
            except Exception:
                pass

            available_quote = await self.exchange.fetch_available_quote_balance()
            position_val = self.position_qty * self.last_price
            total_equity = available_quote + position_val

            unrealized_pnl = (
                (self.last_price - self.avg_entry_price) * self.position_qty
                if self.position_qty > Decimal("0")
                else Decimal("0")
            )
            self.pnl_history.append({
                "time": int(time.time()),
                "value": round(float(self.realized_pnl_usd + unrealized_pnl), 4),
                "price": float(self.last_price),
            })

            if not self.running or self.state == EngineState.HALTED:
                return

            breach = self._check_circuit_breakers(total_equity)
            if breach:
                self.last_reason = f"CIRCUIT BREAKER: {breach}"
                logger.critical(json.dumps({"event": "circuit_breaker_triggered", "reason": breach}))
                await self.emergency_kill_switch(reason=breach)
                return

            in_pos = self.position_qty > Decimal("0")
            if in_pos and self.last_price > self.highest_since_entry:
                self.highest_since_entry = self.last_price

            self.state = EngineState.TRAILING_EXIT if in_pos else EngineState.EVALUATING
            signal = self.strategy.evaluate(
                current_price=self.last_price,
                in_position=in_pos,
                avg_entry_price=self.avg_entry_price,
                highest_price_since_entry=self.highest_since_entry,
            )
            self.last_reason = signal.reason
            self.target_exit_price = signal.min_profit_exit_price
            self.trailing_stop_price = signal.trailing_stop_price

            if signal.action == "BUY" and not in_pos:
                alloc_usd = self._calculate_order_size_usd(available_quote)
                if alloc_usd < Decimal("10"):
                    self.last_reason = "Insufficient capital allocation (< $10 minimum)"
                    self.state = EngineState.IDLE
                    return

                raw_qty = alloc_usd / ticker["ask"]
                self.state = EngineState.ORDER_SUBMITTED
                order = await self.exchange.place_order("buy", "market", raw_qty)
                self.active_order_id = order["id"]
                filled = await self._reconcile_order(order["id"])

                self.position_qty = Decimal(str(filled["filled"]))
                self.avg_entry_price = Decimal(str(filled.get("average") or ticker["ask"]))
                self.highest_since_entry = self.avg_entry_price
                self.target_exit_price = self.strategy.calculate_fee_adjusted_exit(self.avg_entry_price)
                self.active_order_id = None
                self.state = EngineState.POSITION_OPEN
                await self._record_trade("BUY", self.position_qty, self.avg_entry_price, Decimal("0"), order["id"])

            elif signal.action == "SELL" and in_pos:
                self.state = EngineState.ORDER_SUBMITTED
                order = await self.exchange.place_order("sell", "market", self.position_qty)
                self.active_order_id = order["id"]
                filled = await self._reconcile_order(order["id"])

                exit_price = Decimal(str(filled.get("average") or ticker["bid"]))
                gross_pnl = (exit_price - self.avg_entry_price) * self.position_qty
                est_fees = (self.avg_entry_price * self.position_qty * settings.TAKER_FEE_RATE) + (
                    exit_price * self.position_qty * settings.TAKER_FEE_RATE
                )
                net_pnl = gross_pnl - est_fees

                self.realized_pnl_usd += net_pnl
                self.daily_pnl_usd += net_pnl
                self.total_trades += 1
                if net_pnl > Decimal("0"):
                    self.winning_trades += 1

                await self._record_trade("SELL", self.position_qty, exit_price, net_pnl, order["id"])

                self.position_qty = Decimal("0")
                self.avg_entry_price = Decimal("0")
                self.highest_since_entry = Decimal("0")
                self.active_order_id = None
                self.state = EngineState.IDLE
            else:
                self.state = EngineState.POSITION_OPEN if in_pos else EngineState.IDLE

    async def emergency_kill_switch(self, reason: str = "Manual Kill Switch Activated") -> Dict[str, Any]:
        """
        Immediately halts execution, cancels all open orders, and flattens any open position.
        """
        self.running = False
        self.state = EngineState.HALTED
        self.last_reason = f"HALTED: {reason}"

        canceled_count = await self.exchange.cancel_all_open_orders()
        flattened = False

        if self.position_qty > Decimal("0"):
            order = await self.exchange.place_order("sell", "market", self.position_qty)
            filled = await self._reconcile_order(order["id"])
            exit_price = Decimal(str(filled.get("average") or self.last_price))
            net_pnl = (exit_price - self.avg_entry_price) * self.position_qty
            self.realized_pnl_usd += net_pnl
            self.daily_pnl_usd += net_pnl
            await self._record_trade("KILL_SELL", self.position_qty, exit_price, net_pnl, order["id"])
            self.position_qty = Decimal("0")
            self.avg_entry_price = Decimal("0")
            flattened = True

        logger.critical(
            json.dumps({
                "event": "kill_switch_executed",
                "reason": reason,
                "canceled_orders": canceled_count,
                "position_flattened": flattened,
            })
        )
        return {
            "status": "HALTED",
            "canceled_orders": canceled_count,
            "position_flattened": flattened,
            "reason": reason,
        }

    def get_telemetry_snapshot(self) -> Dict[str, Any]:
        unrealized = (
            (self.last_price - self.avg_entry_price) * self.position_qty
            if self.position_qty > Decimal("0")
            else Decimal("0")
        )
        win_rate = (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0.0
        snapshot = {
            "state": self.state.value,
            "running": self.running,
            "dry_run": settings.DRY_RUN,
            "symbol": settings.SYMBOL,
            "last_price": float(self.last_price),
            "last_reason": self.last_reason,
            "position": {
                "qty": float(self.position_qty),
                "avg_entry": float(self.avg_entry_price),
                "target_exit": float(self.target_exit_price),
                "trailing_stop": float(self.trailing_stop_price),
                "unrealized_pnl": round(float(unrealized), 4),
            },
            "metrics": {
                "realized_pnl": round(float(self.realized_pnl_usd), 4),
                "daily_pnl": round(float(self.daily_pnl_usd), 4),
                "total_trades": self.total_trades,
                "win_rate": round(win_rate, 1),
                **self.strategy.latest_metrics,
            },
            "order_book": self.order_book,
            "pnl_curve": list(self.pnl_history)[-60:],
            "recent_trades": list(self.trade_ledger)[:15],
        }
        return snapshot
