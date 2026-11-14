import os
import secrets
from decimal import Decimal
from pydantic import BaseModel, Field, field_validator


class TradingConfig(BaseModel):
    # Environment & Security
    ENV: str = Field(default_factory=lambda: os.getenv("ENV", "production"))
    OWNER_USERNAME: str = Field(default_factory=lambda: os.getenv("OWNER_USERNAME", "admin"))
    OWNER_PASSWORD: str = Field(default_factory=lambda: os.getenv("OWNER_PASSWORD", ""))
    JWT_SECRET: str = Field(
        default_factory=lambda: os.getenv("JWT_SECRET") or secrets.token_hex(32)
    )
    DRY_RUN: bool = Field(
        default_factory=lambda: os.getenv("DRY_RUN", "true").lower() in ("true", "1", "yes")
    )

    # Exchange Credentials (Coinbase Advanced Trade / CCXT)
    EXCHANGE_ID: str = Field(default_factory=lambda: os.getenv("EXCHANGE_ID", "coinbaseadvanced"))
    API_KEY: str = Field(default_factory=lambda: os.getenv("EXCHANGE_API_KEY", ""))
    API_SECRET: str = Field(default_factory=lambda: os.getenv("EXCHANGE_API_SECRET", ""))

    # Infrastructure (Optional Redis & TimescaleDB/Postgres; falls back to in-memory if empty)
    REDIS_URL: str = Field(default_factory=lambda: os.getenv("REDIS_URL", ""))
    DATABASE_URL: str = Field(default_factory=lambda: os.getenv("DATABASE_URL", ""))

    # Market & Capital Allocation
    SYMBOL: str = Field(default_factory=lambda: os.getenv("SYMBOL", "BTC/USDC"))
    ALLOCATION_MODE: str = Field(default_factory=lambda: os.getenv("ALLOCATION_MODE", "FIXED"))  # FIXED or PERCENT
    FIXED_CAPITAL_USD: Decimal = Field(
        default_factory=lambda: Decimal(os.getenv("FIXED_CAPITAL_USD", "100.00"))
    )
    EQUITY_ALLOCATION_PCT: Decimal = Field(
        default_factory=lambda: Decimal(os.getenv("EQUITY_ALLOCATION_PCT", "0.10"))
    )

    # Fee Schedule & Profit Hurdles
    MAKER_FEE_RATE: Decimal = Field(
        default_factory=lambda: Decimal(os.getenv("MAKER_FEE_RATE", "0.0040"))  # 0.40%
    )
    TAKER_FEE_RATE: Decimal = Field(
        default_factory=lambda: Decimal(os.getenv("TAKER_FEE_RATE", "0.0060"))  # 0.60%
    )
    MIN_NET_PROFIT_PCT: Decimal = Field(
        default_factory=lambda: Decimal(os.getenv("MIN_NET_PROFIT_PCT", "0.0035"))  # 0.35% net after round-trip fees
    )

    # Institutional Circuit Breakers
    MAX_DAILY_LOSS_USD: Decimal = Field(
        default_factory=lambda: Decimal(os.getenv("MAX_DAILY_LOSS_USD", "50.00"))
    )
    MAX_DRAWDOWN_PCT: Decimal = Field(
        default_factory=lambda: Decimal(os.getenv("MAX_DRAWDOWN_PCT", "0.05"))  # 5%
    )
    MAX_CONCURRENT_EXPOSURE_USD: Decimal = Field(
        default_factory=lambda: Decimal(os.getenv("MAX_CONCURRENT_EXPOSURE_USD", "500.00"))
    )

    # Strategy Parameters
    CANDLE_INTERVAL_SEC: int = Field(default_factory=lambda: int(os.getenv("CANDLE_INTERVAL_SEC", "15")))
    EMA_FAST: int = Field(default_factory=lambda: int(os.getenv("EMA_FAST", "9")))
    EMA_SLOW: int = Field(default_factory=lambda: int(os.getenv("EMA_SLOW", "21")))
    ATR_PERIOD: int = Field(default_factory=lambda: int(os.getenv("ATR_PERIOD", "14")))
    ATR_TRAIL_MULTIPLIER: Decimal = Field(
        default_factory=lambda: Decimal(os.getenv("ATR_TRAIL_MULTIPLIER", "1.8"))
    )
    MIN_EFFICIENCY_RATIO: Decimal = Field(
        default_factory=lambda: Decimal(os.getenv("MIN_EFFICIENCY_RATIO", "0.30"))  # Chop filter
    )

    @field_validator("OWNER_PASSWORD")
    @classmethod
    def validate_password(cls, v: str) -> str:
        if not v:
            # Generate ephemeral password on boot if not explicitly set, logged once at startup
            return secrets.token_urlsafe(16)
        return v

    @field_validator("ALLOCATION_MODE")
    @classmethod
    def validate_alloc_mode(cls, v: str) -> str:
        mode = v.upper()
        if mode not in ("FIXED", "PERCENT"):
            raise ValueError("ALLOCATION_MODE must be FIXED or PERCENT")
        return mode


settings = TradingConfig()
