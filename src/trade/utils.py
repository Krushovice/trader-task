import pandas as pd


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


def aggregate_ohlcv(df_base: pd.DataFrame, rule: str) -> pd.DataFrame:
    """
    Агрегирует 5m OHLCV в 1h/1d.
    df_base: ['ts','o','h','l','c','v'] с ts в мс (UTC).
    """
    if df_base is None or df_base.empty:
        return pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])

    rule = rule.lower()  # <- важно: '1h', '1d'
    if rule not in ("1h", "1d", "5m", "15m", "3m", "60min"):
        # добавь при необходимости другие частоты
        raise ValueError(f"Unsupported resample rule: {rule}")

    df = df_base.copy()
    df["dt"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df = df.set_index("dt")

    agg = (
        df.resample(rule)  # <= тут теперь lower-case
        .agg({"o": "first", "h": "max", "l": "min", "c": "last", "v": "sum"})
        .dropna()
        .reset_index()
    )
    agg["ts"] = (agg["dt"].astype("int64") // 10**6).astype("int64")  # обратно в мс
    return agg[["ts", "o", "h", "l", "c", "v"]]
