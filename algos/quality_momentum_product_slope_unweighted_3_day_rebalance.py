"""
This strategy is taken from my unserstanding of what I've heard from
AlphaArchitect and their momentum strategy that looks back 12 months and
rebalances every quarter and holds about 30 - 50 stocks.
"""

# todo: use correlation for quality of momentum? https://realpython.com/python310-new-features/#new-functions-in-the-statistics-module

import numpy as np
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
    return AverageDollarVolume(window_length=50, mask=tradables).top(limit)


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
        return data.history(order.asset, "open", 2, "1d")[0], order.amount


def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    context.number_of_stocks = 25

    #  the rebalance frequency in days
    context.rebalance_freq = 3

    #  window length for evaluating momentum in days
    context.window_length = 10

    set_commission(commission.PerTrade(cost=0.0))
    set_slippage(us_equities=NoSlippage())

    schedule_function(
        rebalance,
        date_rules.every_day(),
        time_rules.market_open(minutes=30),
    )

    attach_pipeline(make_pipeline(context), 'pipe')


def make_pipeline(context):
    """TODO"""

    base_universe = T2000US()

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
        screen=base_universe,
        # columns={
        #     'returns': momentum,
        # },
    )

    return pipe


def prepare_bars(assets, data, window_length, minutes_in_day=391):
    """Fetch all bar data for the given assets and filter out assets with insufficient pricing data"""
    minutes_in_window = window_length * minutes_in_day
    minute_prices = data.history(assets, 'open', minutes_in_window, '1m')
    return assets


def compute_quality(assets, window_minutes, data):
    """Quality filter"""
    pass


def compute_momentum(assets, window_minutes, data):
    minute_prices = data.history(assets, 'open', window_minutes, '1m')

    def quality(prices):
        slope, _, r_value, _, _ = stats.linregress(np.arange(prices.size), prices)
        return slope * r_value**2

    return [
        (asset, quality(prices.dropna())) for asset, prices in minute_prices.iteritems()
    ]


def rebalance(context, data):

    print(get_datetime())

    calendar = get_calendar('NYSE')
    if (
        calendar.session_distance(calendar.first_session, get_datetime())
        % context.rebalance_freq
    ):
        return

    # context.output = pipeline_output('pipe')

    assets = pipeline_output('pipe').index
    # TODO: need to compute quality filter here
    momentum = compute_quality_momentum(assets, 390 * context.window_length, data)

    # returns = context.output['returns']

    top_long_names = (
        set(momentum.nlargest(context.number_of_stocks).keys())
        if not momentum.empty
        else set()
    )
    current_names = set(context.portfolio.positions.keys())

    names_to_buy = top_long_names - current_names
    names_to_sell = current_names - top_long_names

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
