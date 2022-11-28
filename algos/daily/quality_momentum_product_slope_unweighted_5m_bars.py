"""
This strategy is taken from my unserstanding of what I've heard from
AlphaArchitect and their momentum strategy that looks back 12 months and
rebalances every quarter and holds about 30 - 50 stocks.
"""
import numpy as np
from scipy import stats

from zipline.api import (
    set_commission,
    schedule_function,
    attach_pipeline,
    get_open_orders,
    get_order,
    pipeline_output,
    order_target_percent,
    record,
    symbol,
    get_datetime,
)
from zipline.finance import commission
from zipline.utils.events import date_rules, time_rules
from zipline.pipeline import Pipeline, CustomFactor
from zipline.pipeline.data.equity_pricing import EquityPricing
from zipline.pipeline.factors import AverageDollarVolume
from zipline.errors import CannotOrderDelistedAsset


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


def my_default_us_equity_mask():

    has_prev_close = EquityPricing.close.latest.notnull()
    has_prev_vol = EquityPricing.volume.latest > 0

    return has_prev_close & has_prev_vol


def _tus(limit):
    tradables = my_default_us_equity_mask()
    return AverageDollarVolume(window_length=50, mask=tradables).top(limit)


def T500US():
    return _tus(500)


def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    context.number_of_stocks = 50
    context.number_of_stocks_for_selection = 30

    #  window length for evaluating momentum in months
    context.window_length = 6

    #  need to keep track of sell orders
    context.sell_orders = set()

    context.names_to_buy = None

    set_commission(commission.PerTrade(cost=0.0))

    attach_pipeline(make_pipeline(context), 'pipe')


def make_pipeline(context):
    """TODO"""

    base_universe = T500US()

    quality_returns = MomentumQuality(
        inputs=[EquityPricing.close],
        window_length=50,
        mask=base_universe,
    )

    pipe = Pipeline(
        screen=base_universe & (quality_returns > 0),
        columns={
            'returns': quality_returns,
        },
    )

    return pipe


def handle_stragglers(context, data):
    """TODO"""

    for order_id in list(context.sell_orders):

        order = get_order(order_id)

        #  check if the order was prior to today
        if get_datetime().date() > order.created.date():

            #  check that the order was not filled
            if order.status != 1 and order.sid in context.portfolio.positions:

                #  try to close it out again
                context.sell_orders.add(order_target_percent(order.sid, 0))

                #  remove the old order id
                context.sell_orders.remove(order_id)


def handle_data(context, data):

    print(get_datetime(), data.current(symbol('A'), 'open'))

    context.output = pipeline_output('pipe')

    returns = context.output['returns']

    top_long_names = (
        set(returns.nlargest(context.number_of_stocks).keys())
        if not returns.empty
        else set()
    )
    current_names = set(context.portfolio.positions.keys())

    context.names_to_buy = top_long_names - current_names
    names_to_sell = current_names - top_long_names

    for name in names_to_sell:

        if data.can_trade(name):
            order_target_percent(name, 0)


def record_vars(context, data):
    record(account_value=context.account.net_liquidation)
