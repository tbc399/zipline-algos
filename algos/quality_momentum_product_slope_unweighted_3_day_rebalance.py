"""
This strategy is taken from my unserstanding of what I've heard from
AlphaArchitect and their momentum strategy that looks back 12 months and
rebalances every quarter and holds about 30 - 50 stocks.
"""

# todo: use correlation for quality of momentum? https://realpython.com/python310-new-features/#new-functions-in-the-statistics-module

import numpy as np
from operator import itemgetter
from multiprocessing import Pool
from multiprocessing import Pool
from scipy import stats
import re
import pandas
from datetime import date

from zipline.api import (
    set_commission,
    set_slippage,
    schedule_function,
    attach_pipeline,
    pipeline_output,
    sid,
    order_target_percent,
    get_open_orders,
    record,
    get_datetime,
)
from zipline import get_calendar
from zipline.finance import commission, slippage
from zipline.utils.events import date_rules, time_rules
from zipline.pipeline import Pipeline, CustomFactor
from zipline.pipeline.data.equity_pricing import EquityPricing
from zipline.pipeline.factors import AverageDollarVolume, Returns
from zipline.errors import CannotOrderDelistedAsset


# class Quality(CustomFactor):
#
#     inputs = [EquityPricing.close]
#
#     def compute(self, today, assets, out, close):
#
#         prices = close.transpose()
#         x = np.arange(self.window_length)
#         output = []
#         for col in prices:
#             _, _, r_value, _, _ = stats.linregress(x, col)
#             output.append(r_value)
#
#         out[:] = output


# class MomentumQuality(CustomFactor):
#     """
#     Notes:
#         - using rate of return and r values does seem to do a little better than just rate of return
#         - slope vs returns is negligible, but it looks like returns is a little better
#         - Probably would be worth looking at one more quality measure, e.g. from Quantitative Momentum book
#     """
#
#     inputs = [EquityPricing.close]
#
#     def compute(self, today, assets, out, close):
#
#         prices = close.transpose()
#         x = np.arange(self.window_length)
#         output = []
#         for col in prices:
#             slope, _, r_value, _, _ = stats.linregress(x, col)
#             output.append(slope * r_value**2)
#
#         out[:] = output


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


class NoSlippage(slippage.SlippageModel):
    def process_order(self, data, order):
        return data.current(order.asset, "open"), order.amount


def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    context.number_of_stocks = 25

    #  the rebalance frequency in days
    # context.rebalance_freq = 5
    context.rebalance_freq = 3

    #  window length for evaluating momentum in days
    context.window_length = 15
    # context.window_length = 45

    # bar size in minutes
    context.bar_size = 30

    set_commission(commission.PerTrade(cost=0.0))
    set_slippage(us_equities=NoSlippage())
    # set_slippage(us_equities=slippage.NoSlippage())

    schedule_function(
        rebalance,
        date_rules.every_day(),
        time_rules.market_open(),
    )

    attach_pipeline(make_pipeline(context), 'pipe')


def make_pipeline(context):
    """TODO"""

    base_universe = T1000US()

    # quality = Quality(
    #     inputs=[EquityPricing.open],
    #     window_length=context.window_length,
    #     mask=base_universe,
    # )
    #
    # momentum = MomentumQuality(
    #     inputs=[EquityPricing.open],
    #     window_length=context.window_length,
    #     mask=base_universe,
    # )

    pipe = Pipeline(
        # screen=base_universe & (quality > 0.9),
        # screen=base_universe & (momentum > 0),
        # screen=base_universe,
        # columns={
        #     'returns': momentum,
        # },
    )

    return pipe


def prepare_bars(assets, data, window_length, bar_size, minutes_in_day=391):
    """Fetch all bar data for the given assets and filter out assets with insufficient pricing data"""
    minutes_in_window = window_length * minutes_in_day
    minute_prices = data.history(assets, 'open', minutes_in_window, '1m')
    bars = [(name, prices.dropna()) for name, prices in minute_prices.iteritems()]
    lookback_length = (minutes_in_day // bar_size) * window_length
    return [
        (name, prices[-lookback_length:])
        for name, prices in bars
        if prices.size >= lookback_length
    ]


def top_volume_filter(assets, data, minutes_in_day=391):
    volume = data.history(assets, 'volume', minutes_in_day, '1m')
    daily_volumes = sorted(
        [(name, data[data != 0].agg(sum)) for name, data in volume.iteritems()],
        key=itemgetter(1),
        reverse=True,
    )
    return [name for name, _ in daily_volumes[:500]]


# def _momentum(args):
#     asset = args[0]
#     prices = args[1]
#     slope, _, r_value, _, _ = stats.linregress(np.arange(prices.size), prices)
#     return asset, r_value, slope * r_value**2


def compute_quality_momentum(asset_prices, quality_threshold: float = 0.0):
    def momentum(prices):
        slope, _, r_value, _, _ = stats.linregress(np.arange(prices.size), prices)
        return r_value, slope * r_value**2

    computed = [(asset, momentum(prices)) for asset, prices in asset_prices]
    return [(asset, mom[1]) for asset, mom in computed if mom[0] > quality_threshold]


def filter_quality(asset_prices, quality_threshold: float = 0.0):
    def quality(prices):
        _, _, r_value, _, _ = stats.linregress(np.arange(prices.size), prices)
        return r_value

    computed = [(asset, quality(prices), prices) for asset, prices in asset_prices]
    return [
        (asset, prices) for asset, qual, prices in computed if qual > quality_threshold
    ]


def compute_raw_momentum(asset_prices):
    def momentum(prices):
        slope, _, _, _, _ = stats.linregress(np.arange(prices.size), prices)
        return slope

    computed = [(asset, momentum(prices)) for asset, prices in asset_prices]
    return [(asset, mom) for asset, mom in computed]


def compute_raw_returns(asset_prices):
    def returns(prices):
        return (prices[-1] - prices[0]) / prices[0]

    computed = [(asset, returns(prices)) for asset, prices in asset_prices]
    return [(asset, mom) for asset, mom in computed]


def rebalance(context, data):

    calendar = get_calendar('NYSE')
    if (
        calendar.session_distance(calendar.first_session, get_datetime())
        % context.rebalance_freq
    ):
        return

    print(str(get_datetime('America/New_York')))

    assets = pipeline_output('pipe').index

    filtered_assets = top_volume_filter(assets, data)

    asset_prices = prepare_bars(
        filtered_assets, data, context.window_length, context.bar_size
    )

    # momentum = compute_quality_momentum(asset_prices, quality_threshold=0.7)
    # momentum = compute_raw_momentum(asset_prices)

    quality_filtered_assets = filter_quality(asset_prices, quality_threshold=0.0)
    momentum = compute_raw_returns(quality_filtered_assets)

    top_names = set(
        [
            x[0]
            for x in sorted(momentum, key=itemgetter(1), reverse=True)[
                : context.number_of_stocks
            ]
        ]
    )

    current_names = set(context.portfolio.positions.keys())

    names_to_buy = top_names - current_names
    names_to_sell = current_names - top_names

    for name in names_to_sell:

        if data.can_trade(name):
            order_target_percent(name, 0)

    pos_size = 1.0 / context.number_of_stocks

    for name in names_to_buy:
        try:
            order_target_percent(name, pos_size)
        except CannotOrderDelistedAsset as error:
            print(error)


# def record_vars(context, data):
#     record(account_value=context.account.net_liquidation)
