import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Deque, Dict, List, Optional

from config import settings


@dataclass
class Candle:
    timestamp: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal


@dataclass
class TradeSignal:
    action: str  # "BUY", "SELL", "HOLD"
    reason: str
    entry_price: Decimal
    min_profit_exit_price: Decimal
    trailing_stop_price: Decimal
    atr: Decimal
    efficiency_ratio: Decimal


class QuantitativeStrategyEngine:
    """
    Multi-timeframe tick aggregator with EMA trend momentum, Kaufman Efficiency Ratio
    macro regime filtering, ATR volatility scaling, and deterministic fee-hurdle exits.
    """

    def __init__(self) -> None:
        self.candles: Deque[Candle] = deque(maxlen=250)
        self._current_candle: Optional[Candle] = None
        self.latest_metrics: Dict[str, float] = {
            "ema_fast": 0.0,
            "ema_slow": 0.0,
            "atr": 0.0,
            "efficiency_ratio": 0.0,
            "fee_hurdle_price": 0.0,
        }

    def ingest_tick(self, price: Decimal, timestamp_sec: Optional[float] = None) -> None:
        now = int(timestamp_sec or time.time())
        bucket = now - (now % settings.CANDLE_INTERVAL_SEC)

        if self._current_candle is None:
            self._current_candle = Candle(bucket, price, price, price, price)
            return

        if bucket == self._current_candle.timestamp:
            if price > self._current_candle.high:
                self._current_candle.high = price
            if price < self._current_candle.low:
                self._current_candle.low = price
            self._current_candle.close = price
        else:
            self.candles.append(self._current_candle)
            self._current_candle = Candle(bucket, price, price, price, price)

    def _compute_ema(self, values: List[Decimal], period: int) -> Decimal:
        if not values:
            return Decimal("0")
        k = Decimal("2") / Decimal(str(period + 1))
        ema = values[0]
        for val in values[1:]:
            ema = (val * k) + (ema * (Decimal("1") - k))
        return ema

    def _compute_atr(self, series: List[Candle], period: int) -> Decimal:
        if len(series) < 2:
            return Decimal("0")
        trs: List[Decimal] = []
        for i in range(1, len(series)):
            curr = series[i]
            prev = series[i - 1]
            tr = max(
                curr.high - curr.low,
                abs(curr.high - prev.close),
                abs(curr.low - prev.close),
            )
            trs.append(tr)
        window = trs[-period:]
        return sum(window) / Decimal(str(len(window)))

    def _compute_efficiency_ratio(self, closes: List[Decimal], period: int = 10) -> Decimal:
        """
        Kaufman Efficiency Ratio (0.0 = pure chop/noise, 1.0 = directional trend).
        Prevents whipsaws in sideways markets.
        """
        if len(closes) <= period:
            return Decimal("0")
        window = closes[-(period + 1):]
        net_change = abs(window[-1] - window[0])
        volatility_sum = sum(abs(window[i] - window[i - 1]) for i in range(1, len(window)))
        if volatility_sum == Decimal("0"):
            return Decimal("0")
        return net_change / volatility_sum

    def calculate_fee_adjusted_exit(self, entry_price: Decimal) -> Decimal:
        """
        Guarantees net profit after round-trip taker/maker fees.
        Exit >= Entry * (1 + EntryFee + ExitFee + MinNetProfit)
        """
        total_friction = (
            settings.TAKER_FEE_RATE
            + settings.MAKER_FEE_RATE
            + settings.MIN_NET_PROFIT_PCT
        )
        return entry_price * (Decimal("1") + total_friction)

    def evaluate(
        self,
        current_price: Decimal,
        in_position: bool,
        avg_entry_price: Decimal = Decimal("0"),
        highest_price_since_entry: Decimal = Decimal("0"),
    ) -> TradeSignal:
        series = list(self.candles)
        if self._current_candle:
            series.append(self._current_candle)

        min_required = max(settings.EMA_SLOW, settings.ATR_PERIOD) + 2
        if len(series) < min_required:
            return TradeSignal(
                action="HOLD",
                reason=f"Warming up indicators ({len(series)}/{min_required} candles)",
                entry_price=current_price,
                min_profit_exit_price=Decimal("0"),
                trailing_stop_price=Decimal("0"),
                atr=Decimal("0"),
                efficiency_ratio=Decimal("0"),
            )

        closes = [c.close for c in series]
        ema_fast = self._compute_ema(closes[-(settings.EMA_FAST * 3):], settings.EMA_FAST)
        ema_slow = self._compute_ema(closes[-(settings.EMA_SLOW * 3):], settings.EMA_SLOW)
        atr = self._compute_atr(series, settings.ATR_PERIOD)
        er = self._compute_efficiency_ratio(closes, period=10)

        min_exit = (
            self.calculate_fee_adjusted_exit(avg_entry_price)
            if in_position
            else self.calculate_fee_adjusted_exit(current_price)
        )
        trail_distance = atr * settings.ATR_TRAIL_MULTIPLIER
        trailing_stop = (
            highest_price_since_entry - trail_distance
            if in_position and highest_price_since_entry > Decimal("0")
            else current_price - trail_distance
        )

        self.latest_metrics = {
            "ema_fast": round(float(ema_fast), 4),
            "ema_slow": round(float(ema_slow), 4),
            "atr": round(float(atr), 4),
            "efficiency_ratio": round(float(er), 4),
            "fee_hurdle_price": round(float(min_exit), 4),
        }

        # Exit Logic when in active position
        if in_position:
            # Never close on trailing profit take unless fee hurdle is cleared
            if current_price >= min_exit and current_price <= trailing_stop:
                return TradeSignal(
                    action="SELL",
                    reason="ATR trailing stop triggered above fee-adjusted profit hurdle",
                    entry_price=avg_entry_price,
                    min_profit_exit_price=min_exit,
                    trailing_stop_price=trailing_stop,
                    atr=atr,
                    efficiency_ratio=er,
                )
            if ema_fast < ema_slow and current_price >= min_exit:
                return TradeSignal(
                    action="SELL",
                    reason="Bearish EMA crossover locked in net profit above fee hurdle",
                    entry_price=avg_entry_price,
                    min_profit_exit_price=min_exit,
                    trailing_stop_price=trailing_stop,
                    atr=atr,
                    efficiency_ratio=er,
                )
            return TradeSignal(
                action="HOLD",
                reason="Holding position; awaiting trailing exit above fee hurdle",
                entry_price=avg_entry_price,
                min_profit_exit_price=min_exit,
                trailing_stop_price=trailing_stop,
                atr=atr,
                efficiency_ratio=er,
            )

        # Entry Logic when IDLE
        if er < settings.MIN_EFFICIENCY_RATIO:
            return TradeSignal(
                action="HOLD",
                reason=f"Regime filter rejected: choppy market (ER {er:.2f} < {settings.MIN_EFFICIENCY_RATIO})",
                entry_price=current_price,
                min_profit_exit_price=min_exit,
                trailing_stop_price=trailing_stop,
                atr=atr,
                efficiency_ratio=er,
            )

        if ema_fast > ema_slow and current_price > ema_fast:
            return TradeSignal(
                action="BUY",
                reason="Bullish EMA momentum + trending macro regime confirmed",
                entry_price=current_price,
                min_profit_exit_price=min_exit,
                trailing_stop_price=trailing_stop,
                atr=atr,
                efficiency_ratio=er,
            )

        return TradeSignal(
            action="HOLD",
            reason="Awaiting bullish EMA crossover alignment",
            entry_price=current_price,
            min_profit_exit_price=min_exit,
            trailing_stop_price=trailing_stop,
            atr=atr,
            efficiency_ratio=er,
        )
