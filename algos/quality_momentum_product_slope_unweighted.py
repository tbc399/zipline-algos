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

from zipline.api import (
    set_commission,
    set_max_leverage,
    schedule_function,
    attach_pipeline,
    cancel_order,
    pipeline_output,
    set_slippage,
    sid,
    order_target_percent,
    order_percent,
    get_open_orders,
    record,
    get_datetime,
)
from zipline.finance import commission, slippage
from zipline.utils.events import date_rules, time_rules
from zipline.pipeline import Pipeline, CustomFactor
from zipline.pipeline.data.equity_pricing import EquityPricing
from zipline.pipeline.factors import (
    Returns,
    AverageDollarVolume,
    RollingPearsonOfReturns,
)
from zipline.errors import CannotOrderDelistedAsset

'''
from zipline.pipeline.filters.fundamentals import (
    IsPrimaryShare,
    IsDepositaryReceipt,
    is_common_stock
)
'''


class MomentumQuality(CustomFactor):
    """
    Notes:
        - using rate of return and r values does seem to do a little better than just rate of return
        - slope vs returns is negligible, but it looks like returns is a little better
        - Probably would be worth looking at one more quality measure, e.g. from Quantitative Momentum book
    """

    inputs = [EquityPricing.close]

    def compute(self, today, assets, out, close):

        prices = close.transpose()
        x = np.arange(self.window_length)
        output = []
        for col in prices:
            slope, _, r_value, _, _ = stats.linregress(x, col)
            output.append(slope * r_value**2)

        out[:] = output


class Quality(CustomFactor):
    """
    Notes:
        - using rate of return and r values does seem to do a little better than just rate of return
        - slope vs returns is negligible, but it looks like returns is a little better
        - Probably would be worth looking at one more quality measure, e.g. from Quantitative Momentum book
    """

    inputs = [EquityPricing.close]

    def compute(self, today, assets, out, close):

        prices = close.transpose()
        x = np.arange(self.window_length)
        output = []
        for col in prices:
            slope, _, r_value, _, _ = stats.linregress(x, col)
            output.append(r_value)

        out[:] = output


class ModReturns(CustomFactor):
    """
    Notes:
        - using rate of return and r values does seem to do a little better than just rate of return
        - slope vs returns is negligible, but it looks like returns is a little better
        - Probably would be worth looking at one more quality measure, e.g. from Quantitative Momentum book
    """

    inputs = [EquityPricing.close]

    def compute(self, today, assets, out, close):
        """21 days to trim off last month"""
        out[:] = (close[-21] - close[0]) / close[0]


def my_default_us_equity_mask():

    has_prev_close = EquityPricing.close.latest.notnull()
    has_prev_vol = EquityPricing.volume.latest > 0

    return has_prev_close & has_prev_vol


def _tus(limit):
    tradables = my_default_us_equity_mask()
    return AverageDollarVolume(window_length=50, mask=tradables).top(limit)


def T500US():
    return _tus(500)


def T100US():
    return _tus(100)


def T1000US():
    return _tus(1000)


def T2000US():
    return _tus(2000)


def T3000US():
    return _tus(3000)


class NoSlip(slippage.SlippageModel):
    def process_order(self, data, order):
        return data.current(order.asset, 'open'), order.amount


# TODO: Now try this with 1 month lookback and 3 day rebalance!!!!!
# TODO: pleasantly surprising that 12 month lookback with 1 month rebalance works quite well
def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    context.number_of_stocks = 30

    #  the rebalance frequency in months
    context.rebalance_freq = 1

    #  window length for evaluating momentum in months
    context.window_length = 6
    # context.window_length = 1
    # context.window_length = 12

    #  need to keep track of sell orders
    context.sell_orders = set()

    context.names_to_buy = None

    #  place stop loss at this percentage of entry price
    context.stop = 0.9
    context.stops = {}

    set_commission(commission.PerTrade(cost=0.0))
    set_slippage(us_equities=NoSlip())
    # set_max_leverage(1)

    schedule_function(rebalance, date_rules.month_start(), time_rules.market_open())
    # schedule_function(rebalance, date_rules.week_start(), time_rules.market_open())
    # schedule_function(check_stops, date_rules.every_day(), time_rules.market_open())

    attach_pipeline(make_pipeline(context), 'pipe')


def months_to_days(months):

    return 21 * months


def make_pipeline(context):
    """TODO"""

    # base_universe = T500US()
    # base_universe = T1000US()
    # base_universe = T2000US()
    base_universe = T3000US()

    quality = Quality(
        inputs=[EquityPricing.close],
        window_length=months_to_days(context.window_length),
        mask=base_universe,
    )

    # quality_returns = MomentumQuality(
    #     inputs=[EquityPricing.close],
    #     window_length=months_to_days(context.window_length),
    #     mask=quality > 0.5,
    # )

    returns = Returns(
        inputs=[EquityPricing.close],
        window_length=months_to_days(context.window_length),
    )

    pipe = Pipeline(
        # screen=base_universe & (quality_returns > 0),
        screen=base_universe & (quality > 0.7),
        # screen=base_universe & (quality < -0.7) & (returns < 0),
        # screen=(base_universe & quality.top(200)) & (returns > 0),
        columns={
            #'returns': quality_returns,
            'returns': returns,
        },
    )

    return pipe


def rebalance(context, data):
    """Rebalance every month"""

    print(get_datetime('America/New_York'))

    context.output = pipeline_output('pipe')

    returns = context.output['returns']
    print(f"Looking at {returns.size} stocks")

    top_names = set(
        returns.sort_values(ascending=False)[: context.number_of_stocks].keys()
    )
    current_names = set(context.portfolio.positions.keys())

    context.names_to_buy = top_names - current_names
    names_to_sell = current_names - top_names

    for name in names_to_sell:

        if data.can_trade(name):
            order_target_percent(name, 0)
            if name in context.stops:
                del context.stops[name]


def handle_data(context, data):

    open_orders = get_open_orders().values()
    open_sell_orders = [
        x for sub_list in open_orders for x in sub_list if x.amount == 0
    ]

    if not len(open_sell_orders) and context.names_to_buy:

        pos_size = 1.0 / context.number_of_stocks
        for name in context.names_to_buy:
            try:
                order_target_percent(name, pos_size)
                context.stops[name] = data.current(name, "close") * context.stop
            except CannotOrderDelistedAsset as error:
                print(error)

        context.names_to_buy = None


def check_stops(context, data):
    """
    My way of simulating stop orders since I can't seem to do it with regular stop
    orders
    """

    for name in context.portfolio.positions.keys():
        stop_price = context.stops.get(name)
        current_price = data.current(name, "open")
        if stop_price and current_price <= stop_price:
            order_target_percent(name, 0)
            del context.stops[name]
            print(f"{name} stopped out")
