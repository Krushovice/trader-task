from typing import Optional


class TrailingStopManager:
    def __init__(
        self,
        side: str,
        trail_pct: float,
        take_profit_pct: Optional[float] = None,
    ):
        self.side = side  # "long" или "short"
        self.trail_pct = trail_pct
        self.take_profit_pct = take_profit_pct
        self.active = False
        self.entry_price: Optional[float] = None
        self.extreme_price: Optional[float] = None

    def activate(self, entry_price: float):
        self.entry_price = entry_price
        self.extreme_price = entry_price
        self.active = True

    def update_price(self, price: float):
        if not self.active:
            return
        if self.side == "long":
            self.extreme_price = max(self.extreme_price, price)
        elif self.side == "short":
            self.extreme_price = min(self.extreme_price, price)

    def should_exit(self, price: float) -> Optional[str]:
        if not self.active or self.entry_price is None:
            return None

        # --- Take Profit ---
        if self.take_profit_pct:
            target_price = (
                self.entry_price * (1 + self.take_profit_pct)
                if self.side == "long"
                else self.entry_price * (1 - self.take_profit_pct)
            )
            if (self.side == "long" and price >= target_price) or (
                    self.side == "short" and price <= target_price
            ):
                return "TP"

        if self.extreme_price is None:
            return None

        # --- Break-even логика ---
        break_even_level = (
            self.entry_price * 1.005 if self.side == "long"
            else self.entry_price * 0.995
        )

        # если достигли 0.5% профита — устанавливаем нижнюю границу в точке входа
        if self.side == "long" and self.extreme_price >= break_even_level:
            trail_stop = max(
                self.entry_price,  # не ниже входа
                self.extreme_price * (1 - self.trail_pct)
            )
            if price <= trail_stop:
                return "TRAIL-BE"
        elif self.side == "short" and self.extreme_price <= break_even_level:
            trail_stop = min(
                self.entry_price,
                self.extreme_price * (1 + self.trail_pct)
            )
            if price >= trail_stop:
                return "TRAIL-BE"

        # обычный трейлинг
        if self.side == "long":
            trail_stop = self.extreme_price * (1 - self.trail_pct)
            if price <= trail_stop:
                return "TRAIL"
        elif self.side == "short":
            trail_stop = self.extreme_price * (1 + self.trail_pct)
            if price >= trail_stop:
                return "TRAIL"

        return None


    def clear(self):
        self.active = False
        self.entry_price = None
        self.extreme_price = None
