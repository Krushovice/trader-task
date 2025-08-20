def normalize_kline(d: dict) -> dict:
    # Приводим к единому формату: start_at, o,h,l,c,v (ms timestamp)
    start_at = d.get("start_at") or d.get("ts")
    return {
        "start_at": int(start_at),
        "open": float(d.get("open", d.get("o"))),
        "high": float(d.get("high", d.get("h"))),
        "low": float(d.get("low", d.get("l"))),
        "close": float(d.get("close", d.get("c"))),
        "volume": float(d.get("volume", d.get("v", 0.0))),
    }
