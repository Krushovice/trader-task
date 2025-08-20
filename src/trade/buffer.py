from collections import deque
import pandas as pd


class BarBuffer:
    def __init__(self, maxlen: int = 1000):
        self._buf = deque(maxlen=maxlen)

    def add(self, k: dict) -> None:
        self._buf.append(
            (k["start_at"], k["open"], k["high"], k["low"], k["close"], k["volume"])
        )

    def to_df(self) -> pd.DataFrame:
        if not self._buf:
            return pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])
        df = pd.DataFrame(self._buf, columns=["ts", "o", "h", "l", "c", "v"])
        return df
