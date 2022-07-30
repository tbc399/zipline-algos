"""
Idea:
- use a 5 and 10 EMA on the open to determine short term trend and use as a guard
- use a 2-3 day RSI to keep from getting in on a wild swing. Filter names that are
    between the boundaries.
- keep hold times short, 1-2 days
"""
import random
import talib
from collections import namedtuple
from zipline.algorithm import TradingAlgorithm
from zipline.api import (
    set_commission,
    set_slippage,
    schedule_function,
    attach_pipeline,
    pipeline_output,
    order_target_percent,
    get_datetime,
)
from zipline.finance import commission, slippage
from zipline.utils.events import date_rules, time_rules
from zipline.pipeline import Pipeline
from zipline.pipeline.data.equity_pricing import USEquityPricing
from zipline.pipeline.factors import AverageDollarVolume


def my_default_us_equity_mask():
    has_prev_close = USEquityPricing.close.latest.notnull()
    has_prev_vol = USEquityPricing.volume.latest > 0
    return has_prev_close & has_prev_vol


def _tus(limit):
    tradables = my_default_us_equity_mask()
    return AverageDollarVolume(window_length=200, mask=tradables).top(limit)


def T500US():
    return _tus(500)


def T1000US():
    return _tus(1000)


class InstantSlippage(slippage.SlippageModel):
    def process_order(self, data, order):
        # Use price from previous bar
        price = data.history(order.sid, "open", 2, "1d")[0]

        # Alternative: Use current bar's open, instead of close
        # price = data.current(order.sid, 'open')

        return price, order.amount


def initialize(context: TradingAlgorithm):

    context.wins = 0
    context.cumm_win = 0
    context.losses = 0
    context.cumm_loss = 0

    context.pnl = 0
    context.rsi_exits = 0
    context.ema_exits = 0
    context.time_exits = 0

    #  initialize the context
    context.max_concentration = 0.04
    context.names_to_buy = None
    context.position_dates = {}

    set_commission(commission.PerTrade(cost=0.0))
    set_slippage(InstantSlippage())

    schedule_function(
        rebalance_start,
        date_rules.every_day(),
        time_rules.market_open(),
    )
    schedule_function(
        exit_positions,
        date_rules.every_day(),
        time_rules.market_close(minutes=1),
    )

    attach_pipeline(make_pipeline(), "pipe")


def make_pipeline():

    base_universe = T500US()

    pipe = Pipeline(
        screen=base_universe,
        columns={"open": USEquityPricing.open.latest},
    )

    return pipe


def exit_positions(context, data):
    for equity, position in context.portfolio.positions.items():
        if equity.symbol == "TWTR":
            print(f"exit TWTR: {get_datetime()}")
        order_target_percent(equity, 0)


PriceData = namedtuple("PriceData", ["key", "price", "ema5", "ema10"])


def screen_and_rank(context, data):

    open_ = pipeline_output("pipe")["open"]

    max_price = context.account.settled_cash * context.max_concentration
    open_ = open_[open_ <= max_price]
    historical_opens = data.history(open_.index, "open", 30, "1d")
    ema5 = historical_opens.apply(lambda col: talib.EMA(col, timeperiod=5))
    ema10 = historical_opens.apply(lambda col: talib.EMA(col, timeperiod=10))

    combo = [
        PriceData(
            key=key,
            price=historical_opens.get(key).iloc[-1],
            ema5=ema5.get(key).iloc[-1],
            ema10=ema10.get(key).iloc[-1],
        )
        for key in historical_opens.keys()
    ]
    rank_filter = [x for x in combo if x.price < x.ema5 and x.ema10 < x.ema5]
    ranking = sorted(
        rank_filter, key=rank_farthest_from_5_ema, reverse=True
    )
    open_order_value = 0

    for name, price, ema5, ema10 in (
        (x.key, x.price, x.ema5, x.ema10) for x in ranking
    ):
        if (
            name not in context.position_dates
            and data.can_trade(name)
            and open_order_value < context.account.settled_cash
        ):
            order_id = order_target_percent(name, context.max_concentration)
            order = context.get_order(order_id)
            if order:
                if name.symbol == "TWTR":
                    print(f"enter TWTR: {get_datetime()}")
                open_order_value += order.amount * price


def rebalance_start(context, data):
    # print(get_datetime().date())

    screen_and_rank(context, data)


def rank_closest_to_10_ema(x: PriceData):
    return abs(x.price - x.ema10)


def rank_farthest_from_5_ema(x: PriceData):
    return percent_change(x.price, x.ema5)


def percent_change(first, second):
    return ((second - first) / first) * 100
