import pandas as pd

def get_coin_metadata() -> dict:
    return {
        "target": {"symbol": "RAY", "timeframe": "1H"},
        "anchors": [
            {"symbol": "SOL", "timeframe": "1H"}
        ]
    }

def generate_signals(candles_target: pd.DataFrame, candles_anchor: pd.DataFrame) -> pd.DataFrame:
    sol = candles_anchor[["open_SOL_1H", "high_SOL_1H", "low_SOL_1H", "close_SOL_1H", "volume_SOL_1H"]].copy()
    sol.columns = ["open", "high", "low", "close", "volume"]  # Temporarily rename for processing

    ray = candles_target.copy()

    sol.set_index("timestamp", inplace=True)
    ray.set_index("timestamp", inplace=True)

    sol = sol.sort_index()
    ray = ray.sort_index()
    common_index = sol.index.intersection(ray.index)
    sol = sol.loc[common_index]
    ray = ray.loc[common_index]

    signal_series = pd.Series("HOLD", index=ray.index)

    rr_ratio = 7
    in_trade = False  # Only one trade at a time

    def is_bullish_wick_signal(row):
        body = abs(row["close"] - row["open"])
        upper_wick = row["high"] - max(row["open"], row["close"])
        lower_wick = min(row["open"], row["close"]) - row["low"]
        wick_height = max(upper_wick, lower_wick)

        if body == 0 or wick_height < 2 * body:
            return False
        if lower_wick < 1.5 * upper_wick:
            return False
        return True

    i = 1
    while i < len(sol) - 7:
        if in_trade:
            i += 1
            continue

        if not is_bullish_wick_signal(sol.iloc[i]):
            i += 1
            continue

        entry_time = sol.index[i + 7]
        if entry_time not in ray.index:
            i += 1
            continue

        entry_price = ray.loc[entry_time, "open"]
        try:
            prev_time = entry_time - pd.Timedelta(hours=1)
            prev_low = ray.loc[prev_time, "low"]
        except KeyError:
            i += 1
            continue

        stop_loss = prev_low + (entry_price - prev_low) * 0.6
        target = entry_price + (entry_price - stop_loss) * rr_ratio

        j = ray.index.get_loc(entry_time) + 1
        exit_signal_given = False

        while j < len(ray):
            current_time = ray.index[j]
            high = ray.loc[current_time, "high"]
            low = ray.loc[current_time, "low"]

            if high >= target:
                signal_series.loc[entry_time] = "BUY"
                signal_series.loc[current_time] = "SELL"
                exit_signal_given = True
                in_trade = False
                i = j + 1  # Skip to next candle after trade closes
                break
            elif low <= stop_loss:
                signal_series.loc[entry_time] = "BUY"
                signal_series.loc[current_time] = "SELL"
                exit_signal_given = True
                in_trade = False
                i = j + 1  # Skip to next candle after trade closes
                break
            j += 1

        if not exit_signal_given:
            signal_series.loc[entry_time] = "BUY"
            in_trade = True
            i = j  # Move to end of candles (next potential signal after trade)

    signal_df = signal_series.reset_index()
    signal_df.columns = ["timestamp", "signal"]
    if signal_df.empty:
        signal_df = pd.DataFrame(columns=["timestamp", "signal"])
    return signal_df


   
