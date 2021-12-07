from zipline.api import (
    schedule_function,
    attach_pipeline,
    pipeline_output,
    order_target_percent,
    get_open_orders,
    get_datetime,
)
from zipline.utils.events import date_rules, time_rules
from zipline.pipeline import Pipeline
from zipline.pipeline.data.equity_pricing import USEquityPricing
from zipline.pipeline.factors import Returns, AverageDollarVolume
from zipline.errors import CannotOrderDelistedAsset


def my_default_us_equity_mask():
    has_prev_close = USEquityPricing.close.latest.notnull()
    has_prev_vol = USEquityPricing.volume.latest > 0
    return has_prev_close & has_prev_vol


def Q500US():
    tradables = my_default_us_equity_mask()
    return AverageDollarVolume(window_length=200, mask=tradables).top(500)


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
    schedule_function(rebalance, date_rules.month_start(), time_rules.market_open())
    attach_pipeline(make_pipeline(context), "pipe")


def months_to_days(months):
    return 21 * months


def make_pipeline(context):
    base_universe = Q500US()
    returns = Returns(
        inputs=[USEquityPricing.close],
        window_length=months_to_days(context.window_length),
        mask=base_universe,
    )
    pipe = Pipeline(
        screen=base_universe,
        columns={"returns": returns},
    )
    return pipe


def rebalance(context, data):
    if get_datetime().month not in range(1, 13, context.rebalance_freq):
        return
    context.output = pipeline_output("pipe")
    returns = context.output["returns"]
    top_long_names = set(returns.nlargest(context.number_of_stocks).keys())
    current_names = set(context.portfolio.positions.keys())
    context.names_to_buy = top_long_names - current_names
    names_to_sell = current_names - top_long_names
    for name in names_to_sell:
        if data.can_trade(name):
            order_target_percent(name, 0)
