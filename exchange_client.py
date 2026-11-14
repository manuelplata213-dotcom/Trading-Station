import asyncio
import hashlib
import hmac
import json
import logging
import time
import uuid
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Optional

import ccxt.async_support as ccxt
from config import settings

logger = logging.getLogger("quant.exchange")


class ExchangeClient:
    """
    Authenticated async exchange connector with deterministic precision rounding,
    rate-limit backoff, and seamless dry-run simulation parity.
    """
    try:
    coinbase_client = RESTClient(
        api_key=os.environ.get(API_KEY"),
        api_secret=os.environ.get("API_SECRET"),
    )
    print("[SUCCESS] Live Exchange API authorized directly in app.py.")
 except Exception as e:
    print(f"[FATAL] Live Exchange API authorization failed: {e}")
    coinbase_client = None

    def __init__(self) -> None:
        self.dry_run = settings.DRY_RUN
        self.symbol = settings.SYMBOL
        self._exchange: Optional[Coinbase.com] = None
        self._market_info: Dict[str, Any] = {}
        self._sim_orders: Dict[str, Dict[str, Any]] = {}
        self._last_price: Decimal = Decimal("0")
        self._sim_balance_quote: Decimal = Decimal("10000.00")
        self._sim_balance_base: Decimal = Decimal("0.00")

    async def initialize(self) -> None:
        exchange_class = getattr(ccxt, settings.EXCHANGE_ID)
        config: Dict[str, Any] = {
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
        }
        if settings.API_KEY and settings.API_SECRET:
            config["apiKey"] = settings.API_KEY
            config["secret"] = settings.API_SECRET.replace("\\n", "\n")

        self._exchange = exchange_class(config)
        await self._with_retry(self._exchange.load_markets)
        self._market_info = self._exchange.market(self.symbol)
        logger.info(
            json.dumps({
                "event": "exchange_initialized",
                "exchange": settings.EXCHANGE_ID,
                "symbol": self.symbol,
                "dry_run": self.dry_run,
            })
        )

    async def close(self) -> None:
        if self._exchange:
            await self._exchange.close()

    async def _with_retry(self, func, *args, max_retries: int = 4, **kwargs) -> Any:
        delay = 0.5
        for attempt in range(1, max_retries + 1):
            t0 = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                latency_ms = round((time.perf_counter() - t0) * 1000, 2)
                if latency_ms > 1000:
                    logger.warning(json.dumps({"event": "high_api_latency", "ms": latency_ms}))
                return result
            except (ccxt.NetworkError, ccxt.RateLimitExceeded, ccxt.ExchangeNotAvailable) as exc:
                if attempt == max_retries:
                    logger.error(json.dumps({"event": "api_retry_exhausted", "error": str(exc)}))
                    raise
                logger.warning(
                    json.dumps({
                        "event": "api_backoff",
                        "attempt": attempt,
                        "delay_sec": delay,
                        "error": str(exc),
                    })
                )
                await asyncio.sleep(delay)
                delay *= 2

    def round_amount(self, amount: Decimal) -> Decimal:
        precision = self._market_info.get("precision", {}).get("amount", 8)
        if isinstance(precision, float) and precision < 1:
            step = Decimal(str(precision))
            return (amount / step).to_integral_value(rounding=ROUND_DOWN) * step
        quant = Decimal("1e-" + str(int(precision)))
        return amount.quantize(quant, rounding=ROUND_DOWN)

    def round_price(self, price: Decimal) -> Decimal:
        precision = self._market_info.get("precision", {}).get("price", 2)
        if isinstance(precision, float) and precision < 1:
            step = Decimal(str(precision))
            return (price / step).to_integral_value(rounding=ROUND_DOWN) * step
        quant = Decimal("1e-" + str(int(precision)))
        return price.quantize(quant, rounding=ROUND_DOWN)

    def sign_internal_payload(self, payload: str) -> str:
        return hmac.new(
            settings.JWT_SECRET.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    async def fetch_ticker_snapshot(self) -> Dict[str, Decimal]:
        ticker = await self._with_retry(self._exchange.fetch_ticker, self.symbol)
        last = Decimal(str(ticker["last"]))
        bid = Decimal(str(ticker.get("bid") or last))
        ask = Decimal(str(ticker.get("ask") or last))
        self._last_price = last
        return {"last": last, "bid": bid, "ask": ask}

    async def fetch_order_book_depth(self, limit: int = 8) -> Dict[str, Any]:
        ob = await self._with_retry(self._exchange.fetch_order_book, self.symbol, limit)
        return {
            "bids": [[float(p), float(q)] for p, q in ob.get("bids", [])[:limit]],
            "asks": [[float(p), float(q)] for p, q in ob.get("asks", [])[:limit]],
        }

    async def fetch_available_quote_balance(self) -> Decimal:
        if self.dry_run:
            return self._sim_balance_quote
        quote_currency = self.symbol.split("/")[1]
        bal = await self._with_retry(self._exchange.fetch_balance)
        free = bal.get("free", {}).get(quote_currency, 0)
        return Decimal(str(free))

    async def place_order(
        self,
        side: str,
        order_type: str,
        amount: Decimal,
        price: Optional[Decimal] = None,
        post_only: bool = False,
    ) -> Dict[str, Any]:
        clean_amount = self.round_amount(amount)
        clean_price = self.round_price(price) if price is not None else None

        if clean_amount <= Decimal("0"):
            raise ValueError(f"Rounded order amount is zero for raw amount {amount}")

        if self.dry_run:
            oid = f"sim-{uuid.uuid4().hex[:12]}"
            exec_price = clean_price if clean_price else self._last_price
            cost = clean_amount * exec_price
            fee_rate = settings.MAKER_FEE_RATE if post_only else settings.TAKER_FEE_RATE
            fee = cost * fee_rate

            if side.lower() == "buy":
                if self._sim_balance_quote < (cost + fee):
                    raise RuntimeError("Insufficient simulated quote balance")
                self._sim_balance_quote -= (cost + fee)
                self._sim_balance_base += clean_amount
            else:
                if self._sim_balance_base < clean_amount:
                    clean_amount = self._sim_balance_base
                self._sim_balance_base -= clean_amount
                self._sim_balance_quote += (cost - fee)

            order_record = {
                "id": oid,
                "symbol": self.symbol,
                "side": side.lower(),
                "type": order_type.lower(),
                "status": "closed",
                "filled": float(clean_amount),
                "amount": float(clean_amount),
                "average": float(exec_price),
                "fee": {"cost": float(fee)},
            }
            self._sim_orders[oid] = order_record
            return order_record

        params: Dict[str, Any] = {}
        if post_only and order_type.lower() == "limit":
            params["postOnly"] = True

        return await self._with_retry(
            self._exchange.create_order,
            self.symbol,
            order_type.lower(),
            side.lower(),
            float(clean_amount),
            float(clean_price) if clean_price else None,
            params,
        )

    async def fetch_order_status(self, order_id: str) -> Dict[str, Any]:
        if self.dry_run:
            return self._sim_orders.get(order_id, {"id": order_id, "status": "canceled", "filled": 0.0})
        return await self._with_retry(self._exchange.fetch_order, order_id, self.symbol)

    async def cancel_all_open_orders(self) -> int:
        if self.dry_run:
            canceled = 0
            for order in self._sim_orders.values():
                if order["status"] == "open":
                    order["status"] = "canceled"
                    canceled += 1
            return canceled

        open_orders = await self._with_retry(self._exchange.fetch_open_orders, self.symbol)
        for order in open_orders:
            await self._with_retry(self._exchange.cancel_order, order["id"], self.symbol)
        return len(open_orders)
