import pandas as pd
import numpy as np

def generate_signals(anchor_data: pd.DataFrame, target_data: pd.DataFrame) -> pd.DataFrame:
    """
    Generates BUY, SELL, or HOLD signals for RAYUSDT based on wick signals on SOLUSDT. 
    """

    # Preprocess timestamps
    anchor_data['timestamp'] = pd.to_datetime(anchor_data['timestamp'])
    target_data['timestamp'] = pd.to_datetime(target_data['timestamp'])
    anchor_data.set_index('timestamp', inplace=True)
    target_data.set_index('timestamp', inplace=True)

    # Align on common index
    common_index = anchor_data.index.intersection(target_data.index)
    anchor_data = anchor_data.loc[common_index]
    target_data = target_data.loc[common_index]

    # Initialize signal column
    signals = pd.DataFrame(index=target_data.index)
    signals["signal"] = "HOLD"

    trade_active = False
    entry_price = None
    stop_loss = None
    target_price = None

    def is_bullish_wick_signal(row):
        body = abs(row["close"] - row["open"])
        upper_wick = row["high"] - max(row["open"], row["close"])
        lower_wick = min(row["open"], row["close"]) - row["low"]
        wick_height = max(upper_wick, lower_wick)
        return body != 0 and wick_height >= 2 * body and lower_wick >= 1.5 * upper_wick

    i = 0
    while i < len(anchor_data) - 2:
        current_time = anchor_data.index[i]

        if not trade_active:
            if is_bullish_wick_signal(anchor_data.iloc[i]):
                entry_time = anchor_data.index[i + 1]

                if entry_time not in target_data.index:
                    i += 1
                    continue

                entry_price = target_data.loc[entry_time, 'open']
                prev_low_time = anchor_data.index[i]

                if prev_low_time not in target_data.index:
                    i += 1
                    continue

                prev_low = target_data.loc[prev_low_time, "low"]
                stop_loss = prev_low + (entry_price - prev_low) * 0.5
                target_price = entry_price + (entry_price - stop_loss) * 3

                signals.loc[entry_time, "signal"] = "BUY"
                trade_active = True
                j = target_data.index.get_loc(entry_time) + 1

                # Hold until SL or TP hit
                while j < len(target_data):
                    row = target_data.iloc[j]
                    ts = target_data.index[j]
                    high = row["high"]
                    low = row["low"]

                    if high >= target_price:
                        signals.loc[ts, "signal"] = "SELL"
                        trade_active = False
                        i = j  # resume from here
                        break
                    elif low <= stop_loss:
                        signals.loc[ts, "signal"] = "SELL"
                        trade_active = False
                        i = j
                        break
                    else:
                        signals.loc[ts, "signal"] = "HOLD"
                    j += 1
            else:
                signals.loc[current_time, "signal"] = "HOLD"
        else:
            signals.loc[current_time, "signal"] = "HOLD"

        i += 1

    # Reset timestamp as column for submission
    signals = signals.reset_index()
    return signals


def get_coin_metadata() -> dict:
    """
    Specifies the target and anchor coins used in this strategy.
    
    Returns:
    {
        "target": {"symbol": "RAYUSDT", "timeframe": "1H"},
        "anchors": [
            {"symbol": "SOLUSDT", "timeframe": "1H"}
        ]
    }
    """
    return {
        "target": {
            "symbol": "RAYUSDT",
            "timeframe": "1H"
        },
        "anchors": [
            {"symbol": "SOLUSDT", "timeframe": "1H"}
        ]
    }
