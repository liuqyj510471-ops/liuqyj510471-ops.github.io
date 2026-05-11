from __future__ import annotations

import re
import json
from dataclasses import asdict, dataclass
from itertools import product
from multiprocessing import cpu_count, freeze_support, get_context
from pathlib import Path
from typing import Any, List, Optional, Tuple
from urllib.request import urlopen
import time 
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay

# Matplotlib 中文字体与负号显示
plt.rcParams["font.sans-serif"] = [
    "Microsoft YaHei",
    "SimHei",
    "Noto Sans CJK SC",
    "PingFang SC",
    "Arial Unicode MS",
]
plt.rcParams["axes.unicode_minus"] = False

try:
    import akshare as ak
except ModuleNotFoundError:
    ak = None

start_time = time.time()

FRED_REAL_YIELD_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFII10"
FOMC_CALENDAR_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
BLS_EMPLOYMENT_URL = "https://www.bls.gov/schedule/news_release/empsit.htm"

BUILTIN_FOMC_DATES = [
    "2025-01-28", "2025-03-18", "2025-05-06", "2025-06-17", "2025-07-29", "2025-09-16", "2025-10-28", "2025-12-09",
    "2026-01-27", "2026-03-17", "2026-04-28", "2026-06-16", "2026-07-28", "2026-09-15", "2026-10-27", "2026-12-08",
    "2027-01-26", "2027-03-16", "2027-04-27", "2027-06-15", "2027-07-27", "2027-09-21", "2027-11-02", "2027-12-14",
]

BUILTIN_NFP_DATES = [
    "2025-01-10", "2025-02-07", "2025-03-07", "2025-04-04", "2025-05-02", "2025-06-06", "2025-07-03", "2025-08-01",
    "2025-09-05", "2025-10-03", "2025-11-07", "2025-12-05",
    "2026-01-09", "2026-02-06", "2026-03-06", "2026-04-03", "2026-05-08", "2026-06-05", "2026-07-03", "2026-08-07",
    "2026-09-04", "2026-10-02", "2026-11-06", "2026-12-04",
    "2027-01-08", "2027-02-05", "2027-03-05", "2027-04-02", "2027-05-07", "2027-06-04", "2027-07-02", "2027-08-06",
    "2027-09-03", "2027-10-08", "2027-11-05", "2027-12-03",
]

_MP_TRAIN_ASSET_DF = None
_MP_TRAIN_MACRO_DF = None
_MP_VALID_ASSET_DF = None
_MP_VALID_MACRO_DF = None
_MP_VALIDATION_START = None
_MP_CONFIG = None


@dataclass(frozen=True)
class RuntimeConfig:
    symbol: str = "sh512890"
    buy_fee_rate: float = 0.0012
    sell_fee_rate_min: float = 0.000
    sell_fee_rate_mid: float = 0.0025
    sell_fee_rate_normal: float = 0.005
    sell_fee_rate_max: float = 0.015
    base_amt: float = 100.0
    min_factor: float = 0.5
    max_factor: float = 5.0
    max_total_factor: float = 8.0
    smooth_window: int = 5
    turtle_breakout_window: int = 20
    kelly_window: int = 60
    volatility_window: int = 20
    volatility_cooldown_days: int = 3
    integral_limit: float = 2.0
    integral_ewm_span: int = 60
    validation_ratio: float = 0.30
    min_segment_size: int = 126
    walk_forward_train_days: int = 504
    walk_forward_test_days: int = 126
    current_shares: float = 1200.50
    peak_price_since_buy: float = 1.65
    event_partial_hedge_bias: float = 0.10
    event_partial_sell_ratio: float = 0.20
    real_yield_cut_threshold: float = 2.0
    real_yield_bonus_threshold: float = 0.0
    macro_cut_multiplier: float = 0.70
    macro_bonus_multiplier: float = 1.20
    max_overfit_ratio: float = 1.5
    param_cache_file: str = r"d:\文档\脚本\算法\redProfit_params_cache_sh512890.json"
    param_cache_version: int = 1
    use_multiprocessing: bool = True
    max_workers: int = 16


@dataclass(frozen=True)
class StrategyParams:
    ma_period: int
    kp: float
    ki: float
    kd: float
    fixed_exit_ratio: float
    trailing_stop_ratio: float


@dataclass(frozen=True)
class SearchSpace:
    ma_periods: list[int]
    kp_values: list[float]
    ki_values: list[float]
    kd_values: list[float]
    fixed_exit_values: list[float]
    trailing_stop_values: list[float]


@dataclass
class SimulationResult:
    annual_return: float
    roi: float
    total_invested: float
    final_value: float
    max_drawdown: float
    calmar_ratio: float
    sortino_ratio: float
    exit_count: int
    hedge_count: int
    prepared_data: Optional[pd.DataFrame] = None
    details: Optional[pd.DataFrame] = None
    exit_events: Optional[List[Tuple[pd.Timestamp, float, str]]] = None
    breakout_events: Optional[List[Tuple[pd.Timestamp, float]]] = None
    hedge_events: Optional[List[Tuple[pd.Timestamp, float, str]]] = None


@dataclass
class FoldResult:
    fold_id: int
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp
    best_params: StrategyParams
    train_return: float
    test_return: float
    train_drawdown: float
    test_drawdown: float
    score: float
    oos_details: pd.DataFrame


@dataclass
class WalkForwardReport:
    folds: list[FoldResult]
    stitched_oos_curve: pd.DataFrame
    parameter_drift: pd.DataFrame
    overfit_ratio: float
    avg_is_return: float
    avg_oos_return: float


def float_range(start: float, stop: float, step: float) -> list[float]:
    return [round(float(value), 4) for value in np.arange(start, stop + step / 2, step)]


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def unique_sorted(values: list[float], digits: int = 4) -> list[float]:
    return sorted({round(float(value), digits) for value in values})


def contiguous_true_spans(boolean_series: pd.Series) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    spans = []
    start = None
    last_idx = None
    for idx, is_true in boolean_series.items():
        if is_true and start is None:
            start = idx
        if not is_true and start is not None and last_idx is not None:
            spans.append((start, last_idx))
            start = None
        last_idx = idx
    if start is not None and last_idx is not None:
        spans.append((start, last_idx))
    return spans


def calculate_max_drawdown(equity_series: pd.Series) -> float:
    if equity_series.empty:
        return 0.0
    running_peak = equity_series.cummax()
    drawdown = (running_peak - equity_series) / running_peak.replace(0, np.nan)
    return float(drawdown.fillna(0.0).max())


def ensure_akshare_available() -> None:
    if ak is None:
        raise ModuleNotFoundError("缺少依赖 `akshare`。请先安装: `pip install akshare`")


class DataProvider:
    def __init__(self, config: RuntimeConfig):
        self.config = config

    def fetch_asset_history(self, symbol: str) -> pd.DataFrame:
        ensure_akshare_available()
        market_df = ak.fund_etf_hist_sina(symbol=symbol)
        market_df["date"] = pd.to_datetime(market_df["date"])
        market_df = market_df.set_index("date").sort_index()
        market_df = market_df[["close"]].rename(columns={"close": "price"})
        market_df["price"] = market_df["price"].astype(float)
        return market_df

    def load_real_yield_series(self) -> pd.Series:
        fred_df = pd.read_csv(FRED_REAL_YIELD_URL)
        if "DATE" in fred_df.columns:
            date_col = "DATE"
        else:
            date_col = "observation_date"
        fred_df[date_col] = pd.to_datetime(fred_df[date_col])
        fred_df["DFII10"] = pd.to_numeric(fred_df["DFII10"], errors="coerce")
        fred_df = fred_df.rename(columns={date_col: "date", "DFII10": "real_yield_10y"})
        fred_df = fred_df.set_index("date").sort_index()
        return fred_df["real_yield_10y"].ffill().fillna(0.0)

    def _load_text_from_url(self, url: str) -> str:
        with urlopen(url, timeout=10) as response:
            return response.read().decode("utf-8", errors="ignore")

    def fetch_fomc_dates_online(self) -> list[pd.Timestamp]:
        try:
            html_text = self._load_text_from_url(FOMC_CALENDAR_URL)
        except Exception:
            return []

        month_map = {
            "January": "01", "February": "02", "March": "03", "April": "04", "May": "05", "June": "06",
            "July": "07", "August": "08", "September": "09", "October": "10", "November": "11", "December": "12",
        }
        parsed_dates = []
        year_blocks = re.findall(r"(20\d{2}) Meeting Dates([\s\S]*?)(?=20\d{2} Meeting Dates|$)", html_text)
        for year_text, block_text in year_blocks:
            for month_name, month_num in month_map.items():
                matches = re.findall(rf"{month_name}\s+(\d{{1,2}})(?:-|–)(\d{{1,2}})", block_text)
                for match in matches:
                    parsed_dates.append(pd.Timestamp(f"{year_text}-{month_num}-{int(match[0]):02d}"))
        return sorted(set(parsed_dates))

    def fetch_nfp_dates_online(self) -> list[pd.Timestamp]:
        try:
            html_text = self._load_text_from_url(BLS_EMPLOYMENT_URL)
        except Exception:
            return []
        date_tokens = re.findall(r"[A-Z][a-z]+\s+\d{1,2},\s+20\d{2}", html_text)
        parsed = pd.to_datetime(date_tokens, errors="coerce")
        return sorted({pd.Timestamp(value).normalize() for value in parsed if not pd.isna(value)})

    def build_event_calendar(self, asset_index: pd.DatetimeIndex) -> pd.DataFrame:
        event_df = pd.DataFrame(index=asset_index)
        event_df["fomc_event"] = False
        event_df["nfp_event"] = False
        event_df["event_guard"] = False
        event_df["event_name"] = ""

        fomc_dates = sorted(set(pd.to_datetime(BUILTIN_FOMC_DATES).tolist() + self.fetch_fomc_dates_online()))
        nfp_dates = sorted(set(pd.to_datetime(BUILTIN_NFP_DATES).tolist() + self.fetch_nfp_dates_online()))

        for event_name, event_dates in [("FOMC", fomc_dates), ("NFP", nfp_dates)]:
            event_col = f"{event_name.lower()}_event"
            for event_date in event_dates:
                event_date = pd.Timestamp(event_date).normalize()
                if event_date not in event_df.index:
                    continue
                previous_trade_day = event_date - BDay(1)
                if previous_trade_day in event_df.index:
                    event_df.loc[previous_trade_day, "event_guard"] = True
                    event_df.loc[previous_trade_day, "event_name"] = event_name
                event_df.loc[event_date, event_col] = True
                event_df.loc[event_date, "event_guard"] = True
                event_df.loc[event_date, "event_name"] = event_name
        return event_df

    def build_macro_frame(self, asset_df: pd.DataFrame) -> pd.DataFrame:
        macro_df = pd.DataFrame(index=asset_df.index)
        macro_df["real_yield_10y"] = self.load_real_yield_series().reindex(asset_df.index).ffill().fillna(0.0)
        macro_df["macro_adjustment"] = np.where(
            macro_df["real_yield_10y"] > self.config.real_yield_cut_threshold,
            self.config.macro_cut_multiplier,
            np.where(
                macro_df["real_yield_10y"] < self.config.real_yield_bonus_threshold,
                self.config.macro_bonus_multiplier,
                1.0,
            ),
        )
        return macro_df.join(self.build_event_calendar(asset_df.index), how="left").fillna({"event_name": ""})


class StrategyEngine:
    def __init__(self, config: RuntimeConfig):
        self.config = config

    def build_macro_score(self, prepared_df: pd.DataFrame) -> pd.Series:
        score_series = pd.Series(50.0, index=prepared_df.index)
        score_series += np.where(prepared_df["real_yield_10y"] < 0, 12.0, 0.0)
        score_series -= np.where(prepared_df["real_yield_10y"] > 2.0, 15.0, 0.0)
        score_series += np.where(prepared_df["turtle_breakout"], 10.0, 0.0)
        score_series -= np.where(prepared_df["panic_drop"], 18.0, 0.0)
        score_series -= np.where(prepared_df["event_guard"], 8.0, 0.0)
        return score_series.clip(lower=0.0, upper=100.0)

    def prepare_feature_frame(self, asset_df: pd.DataFrame, macro_df: pd.DataFrame, params: StrategyParams) -> pd.DataFrame:
        prepared_df = asset_df.copy()
        prepared_df["smooth_price"] = prepared_df["price"].rolling(self.config.smooth_window).mean().rolling(self.config.smooth_window).mean()
        prepared_df["baseline"] = prepared_df["price"].rolling(params.ma_period).mean()
        prepared_df["daily_return"] = prepared_df["price"].pct_change()
        prepared_df["rolling_std"] = prepared_df["daily_return"].rolling(self.config.volatility_window).std()
        prepared_df["turtle_high"] = prepared_df["price"].shift(1).rolling(self.config.turtle_breakout_window).max()
        prepared_df["turtle_breakout"] = prepared_df["price"] > prepared_df["turtle_high"]
        prepared_df["panic_drop"] = (prepared_df["daily_return"] < -0.03) & (prepared_df["daily_return"] < -3 * prepared_df["rolling_std"].shift(1))

        rolling_positive = prepared_df["daily_return"].clip(lower=0).rolling(self.config.kelly_window).mean()
        rolling_negative = prepared_df["daily_return"].clip(upper=0).abs().rolling(self.config.kelly_window).mean()
        win_rate = prepared_df["daily_return"].gt(0).rolling(self.config.kelly_window).mean()
        payoff_ratio = rolling_positive / rolling_negative.replace(0, np.nan)
        kelly_raw = win_rate - (1 - win_rate) / payoff_ratio.replace(0, np.nan)
        prepared_df["kelly_factor"] = (1 + 0.75 * kelly_raw).clip(lower=0.25, upper=1.75).fillna(1.0)

        prepared_df = prepared_df.join(macro_df, how="left")
        prepared_df["real_yield_10y"] = prepared_df["real_yield_10y"].ffill().fillna(0.0)
        prepared_df["macro_adjustment"] = prepared_df["macro_adjustment"].ffill().fillna(1.0)
        prepared_df["event_guard"] = prepared_df["event_guard"].fillna(False)
        prepared_df["event_name"] = prepared_df["event_name"].fillna("")
        prepared_df["macro_score"] = self.build_macro_score(prepared_df)
        prepared_df = prepared_df.dropna(subset=["smooth_price", "baseline", "rolling_std"]).copy()
        return prepared_df

    def simulate(self, asset_df: pd.DataFrame, macro_df: pd.DataFrame, params: StrategyParams, trade_start: Optional[pd.Timestamp] = None, record_history: bool = False, sample_label: str = "FULL") -> SimulationResult:
        prepared_df = self.prepare_feature_frame(asset_df, macro_df, params)
        if trade_start is not None:
            prepared_df = prepared_df.loc[prepared_df.index >= trade_start].copy()
        return self.simulate_prepared(prepared_df, params, record_history=record_history, sample_label=sample_label)

    def simulate_prepared(self, prepared_df: pd.DataFrame, params: StrategyParams, record_history: bool = False, sample_label: str = "FULL") -> SimulationResult:
        if len(prepared_df) < 2:
            return SimulationResult(-999.0, -100.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0, 0)

        total_invested = 0.0
        realized_cash = 0.0
        current_shares = 0.0
        position_lots: list[dict[str, object]] = []
        peak_price = 0.0
        error_sum = 0.0
        last_error = 0.0
        cooldown_left = 0
        exit_count = 0
        hedge_count = 0
        daily_records = []
        exit_events = []
        breakout_events = []
        hedge_events = []

        buy_fee_rate = float(self.config.buy_fee_rate)
        sell_fee_rate_min = float(self.config.sell_fee_rate_min)
        sell_fee_rate_mid = float(self.config.sell_fee_rate_mid)
        sell_fee_rate_normal = float(self.config.sell_fee_rate_normal)
        sell_fee_rate_max = float(self.config.sell_fee_rate_max)

        def _sell_fee_rate_by_holding_days(holding_days: int) -> float:
            if holding_days < 7:
                return sell_fee_rate_max
            if holding_days < 365:
                return sell_fee_rate_normal
            if holding_days < 730:
                return sell_fee_rate_mid
            return sell_fee_rate_min

        def _current_has_lt_7d_lot(asof_date: pd.Timestamp) -> bool:
            for lot in position_lots:
                lot_shares = float(lot["shares"])
                if lot_shares <= 0:
                    continue
                lot_date = pd.Timestamp(lot["date"])
                holding_days = int((asof_date - lot_date).days)
                if holding_days < 7:
                    return True
            return False

        def _sell_shares(asof_date: pd.Timestamp, price: float, shares_to_sell: float) -> float:
            nonlocal current_shares
            remaining = float(shares_to_sell)
            if remaining <= 0 or current_shares <= 0:
                return 0.0
            remaining = min(remaining, current_shares)
            proceeds = 0.0
            lot_idx = 0
            while remaining > 1e-12 and lot_idx < len(position_lots):
                lot = position_lots[lot_idx]
                lot_shares = float(lot["shares"])
                if lot_shares <= 1e-12:
                    lot_idx += 1
                    continue
                sell_from_lot = min(lot_shares, remaining)
                lot_date = pd.Timestamp(lot["date"])
                holding_days = int((asof_date - lot_date).days)
                fee_rate = _sell_fee_rate_by_holding_days(holding_days)
                proceeds += sell_from_lot * price * (1.0 - fee_rate)
                lot["shares"] = lot_shares - sell_from_lot
                remaining -= sell_from_lot
                if float(lot["shares"]) <= 1e-12:
                    lot_idx += 1
            position_lots[:] = [lot for lot in position_lots if float(lot["shares"]) > 1e-12]
            current_shares = sum(float(lot["shares"]) for lot in position_lots)
            return proceeds

        def _liquidation_value(asof_date: pd.Timestamp, price: float) -> float:
            value = 0.0
            for lot in position_lots:
                lot_shares = float(lot["shares"])
                if lot_shares <= 0:
                    continue
                lot_date = pd.Timestamp(lot["date"])
                holding_days = int((asof_date - lot_date).days)
                fee_rate = _sell_fee_rate_by_holding_days(holding_days)
                value += lot_shares * price * (1.0 - fee_rate)
            return value

        for current_date, row in prepared_df.iterrows():
            current_price = float(row["price"])
            current_baseline = float(row["baseline"])
            current_smooth = float(row["smooth_price"])
            current_bias = (current_price - current_baseline) / current_baseline
            action_note = ""

            if current_shares > 0:
                peak_price = max(peak_price, current_price)
                drawdown_from_peak = (peak_price - current_price) / peak_price if peak_price > 0 else 0.0
                should_fixed_exit = current_bias > params.fixed_exit_ratio
                should_trailing_exit = drawdown_from_peak > params.trailing_stop_ratio
                if should_fixed_exit or should_trailing_exit:
                    realized_cash += _sell_shares(current_date, current_price, current_shares)
                    peak_price = 0.0
                    exit_count += 1
                    exit_type = "Fixed Exit" if should_fixed_exit else "Trailing Stop"
                    exit_events.append((current_date, current_price, exit_type))
                    action_note = exit_type

            if bool(row["panic_drop"]):
                cooldown_left = self.config.volatility_cooldown_days
                if not action_note:
                    action_note = "Volatility Guard"

            if bool(row["event_guard"]) and current_shares > 0 and current_bias > self.config.event_partial_hedge_bias:
                partial_sell_shares = current_shares * self.config.event_partial_sell_ratio
                realized_cash += _sell_shares(current_date, current_price, partial_sell_shares)
                hedge_count += 1
                hedge_events.append((current_date, current_price, f"{row['event_name']} Hedge"))
                if not action_note:
                    action_note = "Event Hedge"

            price_error = (current_baseline - current_smooth) / current_baseline
            alpha = 2.0 / (max(2, int(self.config.integral_ewm_span)) + 1.0)
            error_sum = (1.0 - alpha) * error_sum + alpha * price_error
            error_sum = clamp(error_sum, -self.config.integral_limit, self.config.integral_limit)
            delta_error = price_error - last_error
            last_error = price_error

            dynamic_min_factor = self.config.min_factor
            if bool(row["turtle_breakout"]) and current_price > current_baseline:
                dynamic_min_factor = max(dynamic_min_factor, 1.0)
                breakout_events.append((current_date, current_price))

            pid_factor = 1 + params.kp * price_error + params.ki * error_sum + params.kd * delta_error
            pid_factor = clamp(pid_factor, dynamic_min_factor, self.config.max_factor)
            buy_amount = self.config.base_amt * pid_factor
            buy_amount *= float(row["macro_adjustment"])
            buy_amount *= float(row["kelly_factor"])
            buy_amount = clamp(buy_amount, 0.0, self.config.base_amt * self.config.max_total_factor)

            if cooldown_left > 0:
                buy_amount = 0.0
                cooldown_left -= 1
            elif bool(row["event_guard"]):
                guard_cap = self.config.base_amt * dynamic_min_factor * 0.5
                buy_amount = min(buy_amount, guard_cap)
            total_invested += buy_amount

            if buy_amount > 0:
                net_buy_amount = buy_amount * (1.0 - buy_fee_rate)
                buy_shares = net_buy_amount / current_price
                position_lots.append({"date": current_date, "shares": buy_shares, "gross_amount": buy_amount})
                current_shares += buy_shares
                peak_price = max(peak_price, current_price)

            equity_value = realized_cash + _liquidation_value(current_date, current_price)
            daily_records.append(
                {
                    "price": current_price,
                    "baseline": current_baseline,
                    "bias": current_bias,
                    "buy_amount": buy_amount,
                    "real_yield_10y": float(row["real_yield_10y"]),
                    "macro_score": float(row["macro_score"]),
                    "event_guard": bool(row["event_guard"]),
                    "event_name": row["event_name"],
                    "turtle_breakout": bool(row["turtle_breakout"]),
                    "panic_drop": bool(row["panic_drop"]),
                    "kelly_factor": float(row["kelly_factor"]),
                    "equity": equity_value,
                    "has_lot_lt_7d": _current_has_lt_7d_lot(current_date),
                    "sample_label": sample_label,
                    "action_note": action_note,
                }
            )

        details_df = pd.DataFrame(daily_records, index=prepared_df.index)
        last_date = prepared_df.index[-1]
        last_price = float(prepared_df["price"].iloc[-1])
        final_value = realized_cash + _liquidation_value(last_date, last_price)
        roi = final_value / total_invested - 1 if total_invested > 0 else -1.0
        annual_return = (1 + roi) ** (252 / len(prepared_df)) - 1 if total_invested > 0 else -9.99
        max_drawdown = calculate_max_drawdown(details_df["equity"])
        annual_return_decimal = annual_return
        calmar_ratio = annual_return_decimal / max(max_drawdown, 1e-6)
        equity_returns = details_df["equity"].pct_change().dropna()
        downside_returns = equity_returns[equity_returns < 0]
        downside_dev = float(downside_returns.std()) if not downside_returns.empty else 0.0
        downside_dev_annual = downside_dev * np.sqrt(252)
        sortino_ratio = annual_return_decimal / max(downside_dev_annual, 1e-6)
        calmar_ratio = float(np.clip(calmar_ratio, -10.0, 10.0))
        sortino_ratio = float(np.clip(sortino_ratio, -10.0, 10.0))
        return SimulationResult(
            annual_return=annual_return * 100,
            roi=roi * 100,
            total_invested=total_invested,
            final_value=final_value,
            max_drawdown=max_drawdown,
            calmar_ratio=calmar_ratio,
            sortino_ratio=sortino_ratio,
            exit_count=exit_count,
            hedge_count=hedge_count,
            prepared_data=prepared_df if record_history else None,
            details=details_df if record_history else None,
            exit_events=exit_events if record_history else None,
            breakout_events=breakout_events if record_history else None,
            hedge_events=hedge_events if record_history else None,
        )


class OptimizationEngine:
    def __init__(self, config: RuntimeConfig):
        self.config = config
        self.strategy_engine = StrategyEngine(config)

    def build_coarse_search_space(self) -> SearchSpace:
        return SearchSpace(
            ma_periods=[120, 160, 200, 250],
            kp_values=float_range(0.80, 1.80, 0.25),
            ki_values=float_range(-2.80, -1.00, 0.30),
            kd_values=float_range(-12.00, -7.00, 1.00),
            fixed_exit_values=float_range(0.16, 0.32, 0.04),
            trailing_stop_values=float_range(0.05, 0.15, 0.02),
        )

    def build_fine_search_space(self, coarse_result_df: pd.DataFrame) -> SearchSpace:
        top_rows = coarse_result_df.head(2).to_dict("records")
        ma_values = []
        kp_values = []
        ki_values = []
        kd_values = []
        fixed_exit_values = []
        trailing_stop_values = []
        for row in top_rows:
            ma_values.extend(value for value in range(int(row["MA"]) - 20, int(row["MA"]) + 21, 10) if 80 <= value <= 300)
            kp_values.extend(float_range(float(row["Kp"]) - 0.10, float(row["Kp"]) + 0.10, 0.05))
            ki_values.extend(float_range(float(row["Ki"]) - 0.20, float(row["Ki"]) + 0.20, 0.10))
            kd_values.extend(float_range(float(row["Kd"]) - 0.80, float(row["Kd"]) + 0.80, 0.40))
            fixed_exit_values.extend(value for value in float_range(float(row["Fixed_Exit"]) - 0.02, float(row["Fixed_Exit"]) + 0.02, 0.01) if 0.08 <= value <= 0.45)
            trailing_stop_values.extend(value for value in float_range(float(row["Trailing_Stop"]) - 0.02, float(row["Trailing_Stop"]) + 0.02, 0.01) if 0.03 <= value <= 0.20)
        return SearchSpace(
            ma_periods=sorted(set(ma_values)),
            kp_values=unique_sorted(kp_values),
            ki_values=unique_sorted(ki_values),
            kd_values=unique_sorted(kd_values),
            fixed_exit_values=unique_sorted(fixed_exit_values),
            trailing_stop_values=unique_sorted(trailing_stop_values),
        )

    def iter_params(self, search_space: SearchSpace):
        for combo in product(
            search_space.ma_periods,
            search_space.kp_values,
            search_space.ki_values,
            search_space.kd_values,
            search_space.fixed_exit_values,
            search_space.trailing_stop_values,
        ):
            yield StrategyParams(int(combo[0]), float(combo[1]), float(combo[2]), float(combo[3]), float(combo[4]), float(combo[5]))

    def build_train_validation_sets(self, asset_df: pd.DataFrame, macro_df: pd.DataFrame, max_ma_period: int):
        warmup = max_ma_period + self.config.smooth_window * 2 + self.config.kelly_window + self.config.volatility_window
        total_rows = len(asset_df)
        min_validation_size = max(30, int(self.config.min_segment_size))
        split_index = int(total_rows * (1 - self.config.validation_ratio))
        split_index = max(warmup + 1, split_index)
        split_index = min(total_rows - min_validation_size, split_index)
        if split_index <= warmup or total_rows - split_index < min_validation_size:
            raise ValueError("历史数据长度不足，无法进行训练/验证拆分。")
        train_asset_df = asset_df.iloc[:split_index].copy()
        train_macro_df = macro_df.loc[train_asset_df.index].copy()
        valid_asset_df = asset_df.iloc[max(0, split_index - warmup):].copy()
        valid_macro_df = macro_df.loc[valid_asset_df.index].copy()
        validation_start = asset_df.index[split_index]
        return train_asset_df, train_macro_df, valid_asset_df, valid_macro_df, validation_start

    def score_candidate(self, train_asset_df, train_macro_df, valid_asset_df, valid_macro_df, validation_start, params: StrategyParams) -> dict[str, float]:
        train_result = self.strategy_engine.simulate(train_asset_df, train_macro_df, params)
        valid_result = self.strategy_engine.simulate(valid_asset_df, valid_macro_df, params, trade_start=validation_start)
        train_return = train_result.annual_return
        valid_return = valid_result.annual_return
        return_gap = abs(train_return - valid_return)
        calmar_floor = min(train_result.calmar_ratio, valid_result.calmar_ratio)
        sortino_floor = min(train_result.sortino_ratio, valid_result.sortino_ratio)

        score = (
            valid_return * 0.35
            + valid_result.calmar_ratio * 10 * 0.25
            + valid_result.sortino_ratio * 3 * 0.10
            + calmar_floor * 10 * 0.05
            + sortino_floor * 3 * 0.05
            - return_gap * 0.15
            - valid_result.max_drawdown * 100 * 0.10
        )
        return {
            "MA": params.ma_period,
            "Kp": params.kp,
            "Ki": params.ki,
            "Kd": params.kd,
            "Fixed_Exit": params.fixed_exit_ratio,
            "Trailing_Stop": params.trailing_stop_ratio,
            "score": round(score, 4),
            "train_return": round(train_return, 4),
            "validation_return": round(valid_return, 4),
            "return_gap": round(return_gap, 4),
            "validation_drawdown": round(valid_result.max_drawdown * 100, 4),
            "validation_roi": round(valid_result.roi, 4),
            "validation_calmar": round(valid_result.calmar_ratio, 6),
            "validation_sortino": round(valid_result.sortino_ratio, 6),
            "train_calmar": round(train_result.calmar_ratio, 6),
            "train_sortino": round(train_result.sortino_ratio, 6),
        }

    def search_best_parameters(self, stage_name: str, train_asset_df, train_macro_df, valid_asset_df, valid_macro_df, validation_start, search_space: SearchSpace) -> pd.DataFrame:
        params_list = list(self.iter_params(search_space))
        print(f"[{stage_name}] 共 {len(params_list)} 组参数，开始评估...")
        rows = []
        use_parallel = self.config.use_multiprocessing and len(params_list) >= 64 and cpu_count() > 1
        if use_parallel:
            workers = min(self.config.max_workers, max(cpu_count() - 1, 1))
            ctx = get_context("spawn")
            with ctx.Pool(
                processes=workers,
                initializer=_init_worker,
                initargs=(train_asset_df, train_macro_df, valid_asset_df, valid_macro_df, validation_start, self.config),
            ) as pool:
                for idx, result_row in enumerate(pool.imap_unordered(_score_candidate_worker, [asdict(item) for item in params_list]), start=1):
                    rows.append(result_row)
                    if idx % 200 == 0 or idx == len(params_list):
                        print(f"[{stage_name}] 已完成 {idx}/{len(params_list)}")
        else:
            for idx, params in enumerate(params_list, start=1):
                rows.append(self.score_candidate(train_asset_df, train_macro_df, valid_asset_df, valid_macro_df, validation_start, params))
                if idx % 200 == 0 or idx == len(params_list):
                    print(f"[{stage_name}] 已完成 {idx}/{len(params_list)}")
        result_df = pd.DataFrame(rows)
        return result_df.sort_values(by=["score", "validation_return"], ascending=False).reset_index(drop=True)

    def optimize(self, asset_df: pd.DataFrame, macro_df: pd.DataFrame) -> tuple[StrategyParams, pd.DataFrame, pd.DataFrame]:
        coarse_space = self.build_coarse_search_space()
        train_asset_df, train_macro_df, valid_asset_df, valid_macro_df, validation_start = self.build_train_validation_sets(asset_df, macro_df, max(coarse_space.ma_periods))
        coarse_df = self.search_best_parameters("粗搜", train_asset_df, train_macro_df, valid_asset_df, valid_macro_df, validation_start, coarse_space)
        fine_df = self.search_best_parameters("细搜", train_asset_df, train_macro_df, valid_asset_df, valid_macro_df, validation_start, self.build_fine_search_space(coarse_df))
        best_row = fine_df.iloc[0]
        best_params = StrategyParams(int(best_row["MA"]), float(best_row["Kp"]), float(best_row["Ki"]), float(best_row["Kd"]), float(best_row["Fixed_Exit"]), float(best_row["Trailing_Stop"]))
        return best_params, coarse_df, fine_df

    def walk_forward_optimizer(self, asset_df: pd.DataFrame, macro_df: pd.DataFrame) -> WalkForwardReport:
        warmup = max(self.build_coarse_search_space().ma_periods) + self.config.kelly_window + self.config.volatility_window + self.config.smooth_window * 2
        start_index = max(warmup, self.config.walk_forward_train_days)
        folds = []
        stitched_details = []
        fold_id = 1
        while start_index + self.config.walk_forward_test_days <= len(asset_df):
            train_start_idx = start_index - self.config.walk_forward_train_days
            train_end_idx = start_index
            test_end_idx = start_index + self.config.walk_forward_test_days
            train_asset_df = asset_df.iloc[train_start_idx:train_end_idx].copy()
            train_macro_df = macro_df.loc[train_asset_df.index].copy()
            try:
                best_params, _, fine_df = self.optimize(train_asset_df, train_macro_df)
            except ValueError as error:
                if "训练/验证拆分" in str(error):
                    break
                raise

            test_asset_df = asset_df.iloc[max(0, train_end_idx - warmup):test_end_idx].copy()
            test_macro_df = macro_df.loc[test_asset_df.index].copy()
            test_start = asset_df.index[train_end_idx]
            train_result = self.strategy_engine.simulate(train_asset_df, train_macro_df, best_params)
            test_result = self.strategy_engine.simulate(test_asset_df, test_macro_df, best_params, trade_start=test_start, record_history=True, sample_label="OOS")
            oos_details = test_result.details.copy() if test_result.details is not None else pd.DataFrame()
            if not oos_details.empty:
                oos_details["fold_id"] = fold_id
                stitched_details.append(oos_details)
            folds.append(
                FoldResult(
                    fold_id=fold_id,
                    train_start=train_asset_df.index[0],
                    train_end=train_asset_df.index[-1],
                    test_start=test_start,
                    test_end=asset_df.index[test_end_idx - 1],
                    best_params=best_params,
                    train_return=train_result.annual_return,
                    test_return=test_result.annual_return,
                    train_drawdown=train_result.max_drawdown,
                    test_drawdown=test_result.max_drawdown,
                    score=float(fine_df.iloc[0]["score"]),
                    oos_details=oos_details,
                )
            )
            fold_id += 1
            start_index += self.config.walk_forward_test_days

        if not folds:
            return WalkForwardReport([], pd.DataFrame(), pd.DataFrame(), np.nan, np.nan, np.nan)

        stitched_df = pd.concat(stitched_details).sort_index()
        stitched_df = stitched_df[~stitched_df.index.duplicated(keep="last")]
        drift_rows = []
        previous_params = None
        for fold in folds:
            row = asdict(fold.best_params)
            row["fold_id"] = fold.fold_id
            if previous_params is not None:
                for key, value in asdict(fold.best_params).items():
                    row[f"drift_{key}"] = abs(value - previous_params[key])
            previous_params = asdict(fold.best_params)
            drift_rows.append(row)
        drift_df = pd.DataFrame(drift_rows)
        avg_is_return = float(np.mean([fold.train_return for fold in folds]))
        avg_oos_return = float(np.mean([fold.test_return for fold in folds]))
        overfit_ratio = abs(avg_is_return) / abs(avg_oos_return) if abs(avg_oos_return) > 1e-6 else np.inf
        return WalkForwardReport(folds, stitched_df, drift_df, overfit_ratio, avg_is_return, avg_oos_return)


class Plotter:
    def plot(self, simulation: SimulationResult, params: StrategyParams, wfa_report: WalkForwardReport, config: RuntimeConfig) -> None:
        if simulation.prepared_data is None or simulation.details is None:
            return
        prepared_df = simulation.prepared_data
        details_df = simulation.details
        plt.figure(figsize=(16, 14))

        ax1 = plt.subplot(3, 1, 1)
        ax1.plot(prepared_df.index, prepared_df["price"], label="Price", color="gold", alpha=0.7)
        ax1.plot(prepared_df.index, prepared_df["baseline"], label=f"MA{params.ma_period}", color="black", linestyle="--", linewidth=1.0)
        for start, end in contiguous_true_spans(prepared_df["event_guard"]):
            ax1.axvspan(start, end, color="lightgray", alpha=0.25)
        for exit_date, exit_price, exit_type in simulation.exit_events or []:
            color = "red" if exit_type == "Fixed Exit" else "orange"
            marker = "v" if exit_type == "Fixed Exit" else "x"
            ax1.scatter(exit_date, exit_price, color=color, marker=marker, s=80, zorder=5)
        for breakout_date, breakout_price in simulation.breakout_events or []:
            ax1.scatter(breakout_date, breakout_price, color="green", marker="^", s=50, alpha=0.7, zorder=4)
        ax1.set_title(f"五维增强 PID 策略 | {config.symbol}")
        ax1.legend()
        ax1.grid(alpha=0.2)

        ax2 = plt.subplot(3, 1, 2)
        ax2.fill_between(details_df.index, details_df["buy_amount"], color="skyblue", alpha=0.35, label="Total Suggested Amount")
        ax2.axhline(y=config.base_amt, color="gray", linestyle=":", label="Base Amount")
        ax2.grid(alpha=0.2)
        ax2.legend()
        ax2.set_title("当日资金分配")

        ax3 = plt.subplot(3, 1, 3)
        ax3.plot(details_df.index, details_df["equity"], color="black", linewidth=1.3, label="Full Strategy Equity")
        if not wfa_report.stitched_oos_curve.empty:
            ax3.plot(wfa_report.stitched_oos_curve.index, wfa_report.stitched_oos_curve["equity"], color="green", linewidth=1.5, label="WFA OOS Equity")
        ax3.grid(alpha=0.2)
        ax3.legend()
        ax3.set_title("收益曲线: 全样本 vs WFA 样本外")

        plt.tight_layout()
        plt.show()


class InstructionService:
    def __init__(self, config: RuntimeConfig):
        self.config = config

    def build_daily_instruction(self, simulation: SimulationResult, params: StrategyParams) -> dict[str, object]:
        if simulation.details is None:
            raise ValueError("缺少回测明细，无法生成每日指令。")
        latest = simulation.details.iloc[-1]
        latest_bias = float(latest["bias"])
        current_price = float(latest["price"])
        baseline = float(latest["baseline"])
        drawdown = (self.config.peak_price_since_buy - current_price) / self.config.peak_price_since_buy if self.config.peak_price_since_buy > 0 else 0.0
        instruction = "KEEP_BUYING"
        exit_reason = ""
        fee_warning = ""
        if self.config.current_shares > 0:
            if latest_bias > params.fixed_exit_ratio:
                instruction = "SELL_ALL"
                exit_reason = f"触发固定止盈，当前 Bias {latest_bias:.2%} > {params.fixed_exit_ratio:.0%}"
            elif drawdown > params.trailing_stop_ratio:
                instruction = "SELL_ALL"
                exit_reason = f"触发回撤保护，当前回撤 {drawdown:.2%} > {params.trailing_stop_ratio:.0%}"
        if instruction == "SELL_ALL" and bool(latest.get("has_lot_lt_7d", False)):
            fee_warning = "注意：当前部分仓位持有不满 7 天，强制卖出将产生 1.5% 赎回费，请核实。"
        event_warning = "无重大事件静默"
        if bool(latest["event_guard"]):
            event_warning = f"处于 {latest['event_name']} 静默期，买入额降至保守模式"
        return {
            "date": simulation.details.index[-1].strftime("%Y-%m-%d"),
            "price": round(current_price, 4),
            "baseline": round(baseline, 4),
            "instruction": instruction,
            "exit_reason": exit_reason,
            "fee_warning": fee_warning,
            "suggested_amt": round(float(latest["buy_amount"]), 2),
            "macro_score": round(float(latest["macro_score"]), 2),
            "real_yield_10y": round(float(latest["real_yield_10y"]), 2),
            "kelly_factor": round(float(latest["kelly_factor"]), 2),
            "event_warning": event_warning,
            "breakout": bool(latest["turtle_breakout"]),
        }

    def print_daily_instruction(self, result: dict[str, object], params: StrategyParams) -> None:
        print("\n" + "=" * 72)
        print("实战指令")
        print("=" * 72)
        print(f"日期: {result['date']} | 当前价格: {result['price']} | 基准均线: {result['baseline']}")
        print(
            f"参数: MA{params.ma_period}, Kp={params.kp:.2f}, Ki={params.ki:.2f}, Kd={params.kd:.2f}, "
            f"止盈={params.fixed_exit_ratio:.0%}, 回撤={params.trailing_stop_ratio:.0%}"
        )
        print(f"PID 建议额: {result['suggested_amt']} 元 | 宏观评分: {result['macro_score']} | 10Y 实际利率: {result['real_yield_10y']}% | 凯利因子: {result['kelly_factor']}")
        print(f"事件预警: {result['event_warning']}")
        if result["instruction"] == "SELL_ALL":
            print(f"操作指令: 全部卖出 | 原因: {result['exit_reason']}")
            if result.get("fee_warning"):
                print(str(result["fee_warning"]))
        else:
            breakout_text = "是" if result["breakout"] else "否"
            print(f"操作指令: 继续买入 | 海龟突破: {breakout_text}")


def _init_worker(train_asset_df, train_macro_df, valid_asset_df, valid_macro_df, validation_start, config):
    global _MP_TRAIN_ASSET_DF, _MP_TRAIN_MACRO_DF, _MP_VALID_ASSET_DF, _MP_VALID_MACRO_DF, _MP_VALIDATION_START, _MP_CONFIG
    _MP_TRAIN_ASSET_DF = train_asset_df
    _MP_TRAIN_MACRO_DF = train_macro_df
    _MP_VALID_ASSET_DF = valid_asset_df
    _MP_VALID_MACRO_DF = valid_macro_df
    _MP_VALIDATION_START = validation_start
    _MP_CONFIG = config


def _score_candidate_worker(param_dict: dict[str, float]) -> dict[str, float]:
    params = StrategyParams(
        ma_period=int(param_dict["ma_period"]),
        kp=float(param_dict["kp"]),
        ki=float(param_dict["ki"]),
        kd=float(param_dict["kd"]),
        fixed_exit_ratio=float(param_dict["fixed_exit_ratio"]),
        trailing_stop_ratio=float(param_dict["trailing_stop_ratio"]),
    )
    engine = OptimizationEngine(_MP_CONFIG)
    return engine.score_candidate(_MP_TRAIN_ASSET_DF, _MP_TRAIN_MACRO_DF, _MP_VALID_ASSET_DF, _MP_VALID_MACRO_DF, _MP_VALIDATION_START, params)


def print_top_results(title: str, result_df: pd.DataFrame, top_n: int = 3) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)
    for rank, row in enumerate(result_df.head(top_n).itertuples(index=False), start=1):
        print(
            f"Top {rank} | Score={row.score:.2f} | 训练年化={row.train_return:.2f}% | "
            f"验证年化={row.validation_return:.2f}% | 验证卡玛={row.validation_calmar:.3f} | "
            f"验证索提诺={row.validation_sortino:.3f} | 收益差={row.return_gap:.2f}% | 验证回撤={row.validation_drawdown:.2f}%"
        )
        print(
            f"参数: MA{int(row.MA)}, Kp={row.Kp:.2f}, Ki={row.Ki:.2f}, Kd={row.Kd:.2f}, "
            f"止盈={row.Fixed_Exit:.0%}, 回撤={row.Trailing_Stop:.0%}"
        )
        print("-" * 72)


def print_wfa_summary(report: WalkForwardReport) -> None:
    print("\n" + "=" * 72)
    print("WFA 摘要")
    print("=" * 72)
    if not report.folds:
        print("历史数据不足，未生成 WFA 结果。")
        return
    print(f"折数: {len(report.folds)} | 平均样本内年化: {report.avg_is_return:.2f}% | 平均样本外年化: {report.avg_oos_return:.2f}% | 过拟合比率: {report.overfit_ratio:.2f}")
    last_fold = report.folds[-1]
    print(
        f"最近一折: Train {last_fold.train_start.strftime('%Y-%m-%d')} ~ {last_fold.train_end.strftime('%Y-%m-%d')} | "
        f"OOS {last_fold.test_start.strftime('%Y-%m-%d')} ~ {last_fold.test_end.strftime('%Y-%m-%d')} | "
        f"OOS 年化={last_fold.test_return:.2f}%"
    )
    if not report.parameter_drift.empty:
        drift_cols = [col for col in report.parameter_drift.columns if col.startswith("drift_")]
        if drift_cols:
            print(f"平均参数漂移: {report.parameter_drift[drift_cols].mean().round(4).to_dict()}")
        if "drift_ki" in report.parameter_drift.columns:
            ki_drift_mean = float(report.parameter_drift["drift_ki"].mean())
            ki_drift_std = float(report.parameter_drift["drift_ki"].std())
            print(f"Ki 漂移: mean={ki_drift_mean:.4f} | std={ki_drift_std:.4f}")


def build_empty_wfa_report() -> WalkForwardReport:
    return WalkForwardReport(
        folds=[],
        stitched_oos_curve=pd.DataFrame(),
        parameter_drift=pd.DataFrame(),
        overfit_ratio=np.nan,
        avg_is_return=np.nan,
        avg_oos_return=np.nan,
    )


def get_param_cache_path(config: RuntimeConfig) -> Path:
    cache_path = Path(config.param_cache_file)
    if cache_path.is_absolute():
        return cache_path
    return Path(__file__).resolve().with_name(config.param_cache_file)


def validate_strategy_params(params: StrategyParams) -> tuple[bool, str]:
    if not np.isfinite(params.ma_period) or params.ma_period < 20 or params.ma_period > 500:
        return False, "MA 周期超出有效范围"
    for field_name in ["kp", "ki", "kd", "fixed_exit_ratio", "trailing_stop_ratio"]:
        value = getattr(params, field_name)
        if not np.isfinite(value):
            return False, f"{field_name} 非有限数值"
    if not (0.01 <= params.fixed_exit_ratio <= 0.95):
        return False, "fixed_exit_ratio 超出有效范围"
    if not (0.005 <= params.trailing_stop_ratio <= 0.80):
        return False, "trailing_stop_ratio 超出有效范围"
    if abs(params.kp) > 50 or abs(params.ki) > 50 or abs(params.kd) > 100:
        return False, "PID 参数绝对值超出有效范围"
    return True, ""


def serialize_params(params: StrategyParams) -> dict[str, Any]:
    return asdict(params)


def deserialize_params(data: dict[str, Any]) -> StrategyParams:
    return StrategyParams(
        ma_period=int(data["ma_period"]),
        kp=float(data["kp"]),
        ki=float(data["ki"]),
        kd=float(data["kd"]),
        fixed_exit_ratio=float(data["fixed_exit_ratio"]),
        trailing_stop_ratio=float(data["trailing_stop_ratio"]),
    )


def save_cached_params(config: RuntimeConfig, params: StrategyParams, source: str) -> None:
    cache_path = get_param_cache_path(config)
    payload = {
        "cache_version": config.param_cache_version,
        "symbol": config.symbol,
        "saved_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": source,
        "params": serialize_params(params),
    }
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with cache_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
        print(f"参数已保存到本地缓存: {cache_path}")
    except OSError as error:
        print(f"参数缓存写入失败: {error}")


def load_cached_params(config: RuntimeConfig) -> Optional[StrategyParams]:
    cache_path = get_param_cache_path(config)
    if not cache_path.exists():
        return None
    try:
        with cache_path.open("r", encoding="utf-8") as file:
            payload = json.load(file)
    except (OSError, json.JSONDecodeError) as error:
        print(f"参数缓存读取失败，将触发重算: {error}")
        return None

    if not isinstance(payload, dict):
        print("参数缓存格式无效，将触发重算。")
        return None
    if payload.get("cache_version") != config.param_cache_version:
        print("参数缓存版本不匹配，将触发重算。")
        return None
    if payload.get("symbol") != config.symbol:
        print("参数缓存标的不匹配，将触发重算。")
        return None
    try:
        params = deserialize_params(payload["params"])
    except (KeyError, TypeError, ValueError) as error:
        print(f"参数反序列化失败，将触发重算: {error}")
        return None

    is_valid, reason = validate_strategy_params(params)
    if not is_valid:
        print(f"参数缓存校验失败({reason})，将触发重算。")
        return None
    return params


def recalculate_params(
    config: RuntimeConfig,
    optimizer: OptimizationEngine,
    asset_df: pd.DataFrame,
    macro_df: pd.DataFrame,
) -> tuple[StrategyParams, WalkForwardReport]:
    print("正在执行 Walk-Forward Analysis...")
    wfa_report = optimizer.walk_forward_optimizer(asset_df, macro_df)
    print_wfa_summary(wfa_report)

    print("正在执行全样本部署参数优化...")
    deploy_params, coarse_df, fine_df = optimizer.optimize(asset_df, macro_df)
    print_top_results("粗搜结果 Top 3", coarse_df)
    print_top_results("细搜结果 Top 3", fine_df)

    if (
        wfa_report.folds
        and np.isfinite(wfa_report.overfit_ratio)
        and wfa_report.overfit_ratio > config.max_overfit_ratio
    ):
        deploy_params = wfa_report.folds[-1].best_params
        print(
            f"\n过拟合比率 {wfa_report.overfit_ratio:.2f} > {config.max_overfit_ratio:.2f}，"
            f"抛弃全样本最优参数，改用最近一折 WFA 参数进行部署。"
        )

    is_valid, reason = validate_strategy_params(deploy_params)
    if not is_valid:
        raise ValueError(f"重算后参数无效: {reason}")
    save_cached_params(config, deploy_params, source="recalculate_params")
    return deploy_params, wfa_report


def main() -> None:
    config = RuntimeConfig()
    provider = DataProvider(config)
    optimizer = OptimizationEngine(config)
    strategy_engine = StrategyEngine(config)
    plotter = Plotter()
    instruction_service = InstructionService(config)

    print("正在同步行情、宏观因子与事件日历...")
    asset_df = provider.fetch_asset_history(config.symbol)
    macro_df = provider.build_macro_frame(asset_df)
    wfa_report = build_empty_wfa_report()

    deploy_params = load_cached_params(config)
    if deploy_params is not None:
        print("已读取本地缓存参数，跳过重新寻优。")
    else:
        print("本地参数不存在或无效，开始重新计算参数...")
        deploy_params, wfa_report = recalculate_params(config, optimizer, asset_df, macro_df)

    deployment_simulation = strategy_engine.simulate(asset_df, macro_df, deploy_params, record_history=True)
    print(
        f"\n部署参数: MA{deploy_params.ma_period}, Kp={deploy_params.kp:.2f}, Ki={deploy_params.ki:.2f}, Kd={deploy_params.kd:.2f}, "
        f"止盈={deploy_params.fixed_exit_ratio:.0%}, 回撤={deploy_params.trailing_stop_ratio:.0%}"
    )
    print(
        f"全样本表现: 年化={deployment_simulation.annual_return:.2f}% | ROI={deployment_simulation.roi:.2f}% | "
        f"最大回撤={deployment_simulation.max_drawdown * 100:.2f}% | 退出次数={deployment_simulation.exit_count} | 对冲次数={deployment_simulation.hedge_count}"
    )

    plotter.plot(deployment_simulation, deploy_params, wfa_report, config)
    instruction = instruction_service.build_daily_instruction(deployment_simulation, deploy_params)
    instruction_service.print_daily_instruction(instruction, deploy_params)


if __name__ == "__main__":
    start_time = time.time()
    freeze_support()
    try:
        main()
        end_time = time.time()
        print(f"计算耗时: {end_time - start_time:.2f} 秒")
    except Exception as error:
        print(f"计算出错，请检查网络、数据源或参数范围: {error}")
