"""
This strategy is taken from my unserstanding of what I've heard from
AlphaArchitect and their momentum strategy that looks back 12 months and
rebalances every quarter and holds about 30 - 50 stocks.
"""


import numpy as np
from operator import itemgetter

import talib
from scipy import stats

from zipline.api import (
    set_commission,
    set_slippage,
    schedule_function,
    attach_pipeline,
    pipeline_output,
    order_target_percent,
    get_datetime,
    symbol,
)
from zipline import get_calendar
from collections import namedtuple
from zipline.finance import commission, slippage
from zipline.utils.events import date_rules, time_rules
from zipline.pipeline import Pipeline
from zipline.pipeline.data.equity_pricing import EquityPricing
from zipline.pipeline.data.equity_pricing import USEquityPricing
from zipline.pipeline.factors import AverageDollarVolume, Returns
from zipline.errors import CannotOrderDelistedAsset


def my_default_us_equity_mask():

    has_prev_close = EquityPricing.close.latest.notnull()
    has_prev_vol = EquityPricing.volume.latest > 0

    return has_prev_close & has_prev_vol


def _tus(limit):
    tradables = my_default_us_equity_mask()
    return AverageDollarVolume(window_length=10, mask=tradables).top(limit)


def T500US():
    return _tus(500)


def T1000US():
    return _tus(1000)


def T2000US():
    return _tus(2000)


# class InstantSlippage(slippage.SlippageModel):
#     def process_order(self, data, order):
#         # Use price from previous bar
#         price = data.history(order.sid, 'open', 2, '1d')[0]
#
#         # Alternative: Use current bar's open, instead of close
#         # price = data.current(order.sid, 'open')
#
#         return price, order.amount


# class NoSlippage(slippage.SlippageModel):
#     def process_order(self, data, order):
#         return data.current(order.asset, "open"), order.amount


def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    context.number_of_stocks = 20
    # bar size in minutes
    context.bar_size = 30

    set_commission(commission.PerTrade(cost=0.0))
    # set_slippage(us_equities=NoSlippage())
    set_slippage(us_equities=slippage.NoSlippage())

    schedule_function(
        rebalance,
        date_rules.every_day(),
        time_rules.market_open(minutes=5),
    )

    attach_pipeline(make_pipeline(context), 'pipe')


def make_pipeline(context):
    """TODO"""

    base_universe = T1000US()

    return Pipeline(screen=base_universe, columns={"open": USEquityPricing.open.latest})


# def prepare_bars(assets, data, window_length, bar_size, minutes_in_day=391):
#     """Fetch all bar data for the given assets and filter out assets with insufficient pricing data"""
#     minutes_in_window = window_length * minutes_in_day
#     minute_prices = data.history(assets, 'open', minutes_in_window, '1m')
#     bars = [(name, prices.dropna()) for name, prices in minute_prices.iteritems()]
#     lookback_length = (minutes_in_day // bar_size) * window_length
#     return [
#         (name, prices[-lookback_length:])
#         for name, prices in bars
#         if prices.size >= lookback_length
#     ]


# def top_volume_filter(assets, data, minutes_in_day=391):
#     volume = data.history(assets, 'volume', minutes_in_day, '1m')
#     daily_volumes = sorted(
#         [(name, data[data != 0].agg(sum)) for name, data in volume.iteritems()],
#         key=itemgetter(1),
#         reverse=True,
#     )
#     return [name for name, _ in daily_volumes[:500]]
#
#
# def compute_quality_momentum(asset_prices, quality_threshold: float = 0.0):
#     def momentum(prices):
#         slope, _, r_value, _, _ = stats.linregress(np.arange(prices.size), prices)
#         return r_value, slope * r_value**2
#
#     computed = [(asset, momentum(prices)) for asset, prices in asset_prices]
#     return [(asset, mom[1]) for asset, mom in computed if mom[0] > quality_threshold]


# def filter_quality(asset_prices, quality_threshold: float = 0.0):
#     def quality(prices):
#         _, _, r_value, _, _ = stats.linregress(np.arange(prices.size), prices)
#         return r_value
#
#     computed = [(asset, quality(prices), prices) for asset, prices in asset_prices]
#     return [
#         (asset, prices) for asset, qual, prices in computed if qual > quality_threshold
#     ]
#
#
# def compute_raw_momentum(asset_prices):
#     def momentum(prices):
#         slope, _, _, _, _ = stats.linregress(np.arange(prices.size), prices)
#         return slope
#
#     computed = [(asset, momentum(prices)) for asset, prices in asset_prices]
#     return [(asset, mom) for asset, mom in computed]
#
#
# def compute_raw_returns(asset_prices):
#     def returns(prices):
#         return (prices[-1] - prices[0]) / prices[0]
#
#     computed = [(asset, returns(prices)) for asset, prices in asset_prices]
#     return [(asset, mom) for asset, mom in computed]


PriceData = namedtuple("PriceData", ["key", "open", "low_band", "rsi"])


def rebalance(context, data):

    print(str(get_datetime('America/New_York')))

    assets = pipeline_output('pipe')["open"].index
    assets = [symbol('COST')]

    historical_opens = data.history(assets, 'open', 50, '1d')
    bbands = historical_opens.apply(lambda col: talib.BBANDS(col, timeperiod=9))
    rsi = historical_opens.apply(lambda col: talib.RSI(col, timeperiod=5))

    combo = [
        PriceData(
            key=key,
            open=historical_opens.get(key).iloc[-1],
            low_band=bbands.get(key).iloc[2][-1],
            rsi=rsi.get(key).iloc[-1],
        )
        for key in historical_opens.keys()
    ]
    rank_filter = [x for x in combo if x.open < x.low_band and x.rsi > 20]
    # ranking = sorted(rank_filter, key=rank_farthest_from_5_ema, reverse=True)
    open_order_value = 0

    for name, open_, low_band, rsi in (
        (x.key, x.open, x.low_band, x.rsi) for x in rank_filter
    ):
        if (
            # name not in context.position_dates
            data.can_trade(name)
            and open_order_value < context.account.settled_cash
        ):
            x = 5
            # order_id = order_target_percent(name, context.max_concentration)
            # order = context.get_order(order_id)
            # if order:
            #     open_order_value += order.amount * price
