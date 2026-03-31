"""BUY / HOLD / SELL signal engine for JSE SENS disclosures.

Signals are derived from two inputs:
- Price trend (short vs. long moving average), when price history is available.
- Sentiment score, which can be supplied directly or derived from a disclosure row
  using the ``category`` and ``analyst_relevant`` fields already stored in SQLite.

No external paid APIs are required.  All logic depends only on data already
present in the pipeline database.

Usage::

    from mvp_sens.signals import generate_signal, generate_signal_for_disclosure

    # With price data
    result = generate_signal({"prices": [100, 101, 103, 105, 104, 107]})
    # {"signal": "BUY", "confidence": 62.5, "reason": "Uptrend + neutral sentiment"}

    # From a disclosure DB row (no price data needed)
    result = generate_signal_for_disclosure({"sens_id": "X1", "company": "ACME",
                                             "category": "financial_results",
                                             "analyst_relevant": 1})
    # {"sens_id": "X1", "company": "ACME", "signal": "BUY", ...}
"""
from __future__ import annotations

from typing import Any

# ── Tunable constants (no hardcoding in callers) ──────────────────────────────

SHORT_MA_WINDOW: int = 5          # periods for short moving average
LONG_MA_WINDOW: int = 20          # periods for long moving average

#: Weight of sentiment in composite score when price data is available (0–1).
SENTIMENT_WEIGHT: float = 0.4
#: Weight of price trend in composite score when price data is available (0–1).
TREND_WEIGHT: float = 0.6

#: Composite score must exceed this to trigger a BUY signal.
BUY_THRESHOLD: float = 0.15
#: Composite score must fall below this to trigger a SELL signal.
SELL_THRESHOLD: float = -0.15

#: Baseline sentiment score per disclosure category.
CATEGORY_SENTIMENT: dict[str, float] = {
    "financial_results": 0.30,
    "earnings_update": 0.25,
    "trading_statement": 0.00,
    "other": 0.00,
}

#: Extra sentiment boost applied when ``analyst_relevant`` is true.
ANALYST_RELEVANT_BOOST: float = 0.10


# ── Internal helpers ──────────────────────────────────────────────────────────


def _moving_average(prices: list[float], window: int) -> float | None:
    """Return the simple moving average of the last *window* prices.

    Returns ``None`` when the price list is shorter than *window*.
    """
    if len(prices) < window:
        return None
    return sum(prices[-window:]) / window


# ── Public API ────────────────────────────────────────────────────────────────


def generate_signal(
    price_data: dict[str, Any],
    sentiment_score: float | None = None,
) -> dict[str, Any]:
    """Generate a BUY / HOLD / SELL signal from price data and optional sentiment.

    Parameters
    ----------
    price_data:
        Dict with at least ``"prices"`` (``list[float]``, newest value last).
        Optional keys:

        - ``"short_window"`` (``int``): override :data:`SHORT_MA_WINDOW`.
        - ``"long_window"`` (``int``): override :data:`LONG_MA_WINDOW`.

    sentiment_score:
        Float in ``[-1.0, 1.0]``.  Positive = bullish, negative = bearish.
        Pass ``None`` (default) to treat sentiment as neutral.

    Returns
    -------
    dict
        Keys: ``"signal"`` (``"BUY" | "HOLD" | "SELL"``),
        ``"confidence"`` (float 0–100), ``"reason"`` (str).
    """
    prices: list[float] = list(price_data.get("prices") or [])
    short_window: int = int(price_data.get("short_window") or SHORT_MA_WINDOW)
    long_window: int = int(price_data.get("long_window") or LONG_MA_WINDOW)

    # ── Trend component ───────────────────────────────────────────────────────
    short_ma = _moving_average(prices, short_window)
    long_ma = _moving_average(prices, long_window)

    if short_ma is not None and long_ma is not None and long_ma != 0:
        raw_trend = (short_ma - long_ma) / long_ma
        # Scale to [-1, 1]; ±10 % deviation maps to ±1
        trend_score = max(-1.0, min(1.0, raw_trend * 10.0))
        trend_desc = (
            "uptrend" if trend_score > 0.0
            else "downtrend" if trend_score < 0.0
            else "flat trend"
        )
        has_price = True
    else:
        trend_score = 0.0
        trend_desc = "insufficient price history"
        has_price = False

    # ── Sentiment component ───────────────────────────────────────────────────
    sent = 0.0 if sentiment_score is None else max(-1.0, min(1.0, sentiment_score))
    sent_desc = (
        "positive sentiment" if sent > 0.05
        else "negative sentiment" if sent < -0.05
        else "neutral sentiment"
    )

    # ── Composite score ───────────────────────────────────────────────────────
    if has_price:
        composite = trend_score * TREND_WEIGHT + sent * SENTIMENT_WEIGHT
    else:
        # No price history — fall back to sentiment-only
        composite = sent

    # ── Signal & confidence ───────────────────────────────────────────────────
    confidence = round(min(100.0, 50.0 + abs(composite) * 50.0), 1)

    if composite >= BUY_THRESHOLD:
        signal = "BUY"
        reason = (
            f"{trend_desc.capitalize()} + {sent_desc}"
            if has_price
            else sent_desc.capitalize()
        )
    elif composite <= SELL_THRESHOLD:
        signal = "SELL"
        reason = (
            f"{trend_desc.capitalize()} + {sent_desc}"
            if has_price
            else sent_desc.capitalize()
        )
    else:
        signal = "HOLD"
        reason = (
            f"Mixed signals ({trend_desc}, {sent_desc})"
            if has_price
            else f"Neutral — {sent_desc}"
        )

    return {"signal": signal, "confidence": confidence, "reason": reason}


def derive_sentiment_from_disclosure(disclosure: dict[str, Any]) -> float:
    """Derive a sentiment score in ``[-1.0, 1.0]`` from a disclosure dict.

    Uses the ``category`` and ``analyst_relevant`` fields that are already
    persisted in the ``sens_financial_announcements`` table.

    Parameters
    ----------
    disclosure:
        Any mapping that contains ``"category"`` (str) and
        ``"analyst_relevant"`` (int or bool).

    Returns
    -------
    float
        Sentiment score in ``[-1.0, 1.0]``.
    """
    category: str = (disclosure.get("category") or "other").lower()
    analyst_relevant: bool = bool(disclosure.get("analyst_relevant"))

    base: float = CATEGORY_SENTIMENT.get(category, 0.0)
    if analyst_relevant:
        base = min(1.0, base + ANALYST_RELEVANT_BOOST)
    return base


def generate_signal_for_disclosure(disclosure: dict[str, Any]) -> dict[str, Any]:
    """Convenience wrapper: generate a signal directly from a disclosure DB row.

    No price data is required.  Sentiment is derived from the ``category``
    and ``analyst_relevant`` fields in the row.

    Parameters
    ----------
    disclosure:
        Dict (or ``sqlite3.Row``) with at least ``"sens_id"``, ``"company"``,
        ``"category"``, and ``"analyst_relevant"``.

    Returns
    -------
    dict
        Includes all keys from :func:`generate_signal` plus ``"sens_id"``
        and ``"company"``.
    """
    sentiment = derive_sentiment_from_disclosure(disclosure)
    result = generate_signal({}, sentiment_score=sentiment)
    return {
        "sens_id": disclosure.get("sens_id", ""),
        "company": disclosure.get("company", ""),
        **result,
    }
