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
    schedule_function,
    attach_pipeline,
    pipeline_output,
    sid,
    order_target_percent,
    get_open_orders,
    record,
    get_datetime,
)
from zipline.finance import commission
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
            # output.append(r_value)

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
            # output.append(slope * r_value**2)
            output.append(r_value)

        out[:] = output


def my_default_us_equity_mask():

    has_prev_close = EquityPricing.close.latest.notnull()
    has_prev_vol = EquityPricing.volume.latest > 0

    return has_prev_close & has_prev_vol


def _tus(limit):
    tradables = my_default_us_equity_mask()
    return AverageDollarVolume(window_length=200, mask=tradables).top(limit)


def T500US():
    return _tus(500)


def T100US():
    return _tus(100)


def T1000US():
    return _tus(1000)


def T2000US():
    return _tus(2000)


def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    context.number_of_stocks = 25

    #  the rebalance frequency in months
    context.rebalance_freq = 1

    #  window length for evaluating momentum in months
    context.window_length = 6

    #  need to keep track of sell orders
    context.sell_orders = set()

    context.names_to_buy = None

    set_commission(commission.PerTrade(cost=0.0))

    schedule_function(rebalance, date_rules.month_start(), time_rules.market_open())

    attach_pipeline(make_pipeline(context), 'pipe')


def months_to_days(months):

    return 21 * months


def make_pipeline(context):
    """TODO"""

    base_universe = T500US()
    # base_universe = T1000US()

    quality = Quality(
        inputs=[EquityPricing.close],
        window_length=months_to_days(context.window_length),
        mask=base_universe,
    )

    quality_returns = MomentumQuality(
        inputs=[EquityPricing.close],
        window_length=months_to_days(context.window_length),
        mask=quality > 0.5,
    )

    pipe = Pipeline(
        screen=base_universe & (quality_returns > 0),
        columns={
            'returns': quality_returns,
        },
    )

    return pipe


def rebalance(context, data):
    """Rebalance every month"""

    if get_datetime().month not in range(1, 13, context.rebalance_freq):
        return

    context.output = pipeline_output('pipe')

    returns = context.output['returns']

    top_names = set(
        returns.sort_values(ascending=False)[: context.number_of_stocks].keys()
    )
    current_names = set(context.portfolio.positions.keys())

    context.names_to_buy = top_names - current_names
    names_to_sell = current_names - top_names

    for name in names_to_sell:

        if data.can_trade(name):
            order_target_percent(name, 0)

    # position_size = 1.0 / context.number_of_stocks

    # for name in names_to_buy:

    #    if data.can_trade(name):
    #        order_target_percent(name, position_size)


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
            except CannotOrderDelistedAsset as error:
                print(error)

        context.names_to_buy = None


def record_vars(context, data):
    record(account_value=context.account.net_liquidation)
