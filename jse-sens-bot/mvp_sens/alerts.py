"""Basic alerting system for JSE SENS asset monitoring.

Checks a snapshot of asset data against configurable thresholds and returns
a list of triggered alerts.  Alerts are returned as plain dicts so they can
be serialised to JSON, stored in the DB, or displayed in the UI without
further coupling.

No external paid APIs are required.

Usage::

    from mvp_sens.alerts import check_alerts

    alerts = check_alerts(
        {"symbol": "ACM", "price": 110.0, "prev_price": 100.0},
        {"pct_change": 5.0},
    )
    # [{"type": "pct_change", "symbol": "ACM",
    #   "message": "ACM moved up 10.0% (threshold: 5.0%)", "value": 10.0}]
"""
from __future__ import annotations

from typing import Any

# ── Default threshold constants (override by passing values in *thresholds*) ──

#: Minimum absolute % price move (relative to prev_price) to trigger an alert.
DEFAULT_PCT_CHANGE_THRESHOLD: float = 5.0

#: Minimum volume / avg_volume ratio to trigger a volume-spike alert.
DEFAULT_VOLUME_SPIKE_FACTOR: float = 2.0


# ── Public API ────────────────────────────────────────────────────────────────


def check_alerts(
    asset_data: dict[str, Any],
    thresholds: dict[str, Any],
) -> list[dict[str, Any]]:
    """Check whether any alert conditions are triggered for an asset snapshot.

    Parameters
    ----------
    asset_data:
        Dict describing the current state of an asset.  Recognised keys:

        - ``"symbol"`` (str): asset identifier (e.g. ``"ACM"``).
        - ``"price"`` (float): current price.
        - ``"prev_price"`` (float, optional): previous period close.
        - ``"volume"`` (float, optional): current period volume.
        - ``"avg_volume"`` (float, optional): average volume baseline.

    thresholds:
        Dict of alert conditions to check.  All keys are optional:

        - ``"price_above"`` (float): alert when price **crosses above** level.
        - ``"price_below"`` (float): alert when price **drops below** level.
        - ``"pct_change"`` (float): alert when ``|% change|`` ≥ this value.
          Defaults to :data:`DEFAULT_PCT_CHANGE_THRESHOLD` when ``prev_price``
          is present and no explicit value is supplied.
        - ``"volume_spike_factor"`` (float): alert when
          ``volume / avg_volume`` ≥ this value.
          Defaults to :data:`DEFAULT_VOLUME_SPIKE_FACTOR`.

    Returns
    -------
    list[dict]
        Each triggered alert is a dict with keys:

        - ``"type"`` (str): one of ``"price_above"``, ``"price_below"``,
          ``"pct_change"``, ``"volume_spike"``.
        - ``"symbol"`` (str): asset identifier.
        - ``"message"`` (str): human-readable description.
        - ``"value"`` (float): the triggering value (price, %, or ratio).
    """
    triggered: list[dict[str, Any]] = []

    symbol: str = str(asset_data.get("symbol") or "UNKNOWN")
    price = _to_float(asset_data.get("price"))
    prev_price = _to_float(asset_data.get("prev_price"))
    volume = _to_float(asset_data.get("volume"))
    avg_volume = _to_float(asset_data.get("avg_volume"))

    if price is None:
        return triggered

    # ── Price threshold: above ─────────────────────────────────────────────
    price_above = _to_float(thresholds.get("price_above"))
    if price_above is not None and price > price_above:
        triggered.append({
            "type": "price_above",
            "symbol": symbol,
            "message": (
                f"{symbol} price {price:.2f} crossed above threshold {price_above:.2f}"
            ),
            "value": price,
        })

    # ── Price threshold: below ─────────────────────────────────────────────
    price_below = _to_float(thresholds.get("price_below"))
    if price_below is not None and price < price_below:
        triggered.append({
            "type": "price_below",
            "symbol": symbol,
            "message": (
                f"{symbol} price {price:.2f} dropped below threshold {price_below:.2f}"
            ),
            "value": price,
        })

    # ── % change alert ─────────────────────────────────────────────────────
    pct_threshold = _to_float(
        thresholds.get("pct_change", DEFAULT_PCT_CHANGE_THRESHOLD)
    )
    if prev_price is not None and prev_price != 0.0 and pct_threshold is not None:
        pct_change = (price - prev_price) / prev_price * 100.0
        if abs(pct_change) >= pct_threshold:
            direction = "up" if pct_change > 0 else "down"
            triggered.append({
                "type": "pct_change",
                "symbol": symbol,
                "message": (
                    f"{symbol} moved {direction} {abs(pct_change):.1f}% "
                    f"(threshold: {pct_threshold:.1f}%)"
                ),
                "value": round(pct_change, 2),
            })

    # ── Volume spike alert ─────────────────────────────────────────────────
    spike_factor = _to_float(
        thresholds.get("volume_spike_factor", DEFAULT_VOLUME_SPIKE_FACTOR)
    )
    if (
        volume is not None
        and avg_volume is not None
        and avg_volume > 0.0
        and spike_factor is not None
    ):
        ratio = volume / avg_volume
        if ratio >= spike_factor:
            triggered.append({
                "type": "volume_spike",
                "symbol": symbol,
                "message": (
                    f"{symbol} volume spike: {volume:,.0f} "
                    f"({ratio:.1f}x avg, threshold: {spike_factor:.1f}x)"
                ),
                "value": round(ratio, 2),
            })

    return triggered


# ── Internal helpers ──────────────────────────────────────────────────────────


def _to_float(value: Any) -> float | None:
    """Safely coerce *value* to float, returning ``None`` on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
