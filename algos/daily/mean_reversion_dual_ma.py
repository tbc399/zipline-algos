"""
Idea:
- determine the mean with something like a 5 day EMA on the open
- determine the trend with a 10 day EMA to compare with the 5 day EMA
- Look for opening at or below both MAs in up trend
- Look for an RSI that is still above the oversold threshold
- could get out when it reached the "mean" or maybe when 2 period RSI breaks over threshold
- rank on biggest move from EMA (slow or fast)?
- rank on biggest diff between fast and slow EMA
- Maybe add a larger EMA (e.g. 26 day) as a "guard"?
Notes:
- Look at instances of losses to see if there's a common thread
- Look at instances of winners to see what's the common thread
"""
import talib
from collections import namedtuple
from pandas import Timedelta
import zipline.protocol
from zipline.algorithm import TradingAlgorithm
from zipline.api import (
    set_commission,
    symbol,
    schedule_function,
    record,
    attach_pipeline,
    pipeline_output,
    order_target_percent,
    get_open_orders,
    get_datetime,
)
from zipline.finance import commission
from zipline.utils.events import date_rules, time_rules
from zipline.protocol import BarData
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

    schedule_function(
        rebalance_start,
        date_rules.every_day(),
        time_rules.market_open(minutes=1),
    )

    attach_pipeline(make_pipeline(context), "pipe")


def make_pipeline():

    base_universe = T1000US()

    pipe = Pipeline(
        screen=base_universe,
        columns={"open": USEquityPricing.open.latest},
    )

    return pipe


def evaluate_current_positions(context, data):

    for equity, position in context.portfolio.positions.items():
        historical_opens = data.history(equity, "open", 30, "1d")
        ema10 = talib.EMA(historical_opens, timeperiod=10)
        if equity not in context.position_dates:
            order_target_percent(equity, 0)
            continue
        if (
            context.trading_calendar.sessions_window(get_datetime().date(), -5)[0]
            >= context.position_dates[equity][0]
        ):
            if ema10.iloc[-1] >= historical_opens.iloc[-1]:
                order_id = order_target_percent(equity, 0)
                order = context.get_order(order_id)
                if not order:
                    continue
                pnl = (
                    historical_opens.iloc[-1] * -order.amount
                ) - context.position_dates[equity][1]
                p_change = round((pnl / context.position_dates[equity][1]) * 100, 2)
                if pnl > 0:
                    context.cumm_win += pnl
                    # print(
                    #     "win",
                    #     context.position_dates[equity][0].date(),
                    #     get_datetime().date(),
                    #     equity.symbol,
                    #     "time",
                    #     p_change,
                    # )
                else:
                    context.losses += 1
                    context.cumm_loss += -pnl
                    # print(
                    #     "loss",
                    #     context.position_dates[equity][0].date(),
                    #     get_datetime().date(),
                    #     equity.symbol,
                    #     "time",
                    #     p_change,
                    # )
                context.pnl += pnl
                del context.position_dates[equity]
                continue

        rsi = talib.RSI(historical_opens, timeperiod=2)
        if rsi.iloc[-1] >= 80:
            order_id = order_target_percent(equity, 0)
            order = context.get_order(order_id)
            if not order:
                continue
            pnl = (historical_opens.iloc[-1] * -order.amount) - context.position_dates[
                equity
            ][1]
            p_change = round((pnl / context.position_dates[equity][1]) * 100, 2)
            if pnl > 0:
                context.wins += 1
                context.cumm_win += pnl
                # print(
                #     "win",
                #     context.position_dates[equity][0].date(),
                #     get_datetime().date(),
                #     equity.symbol,
                #     "RSI",
                #     p_change,
                # )
            else:
                context.losses += 1
                context.cumm_loss += -pnl
                # print(
                #     "loss",
                #     context.position_dates[equity][0].date(),
                #     get_datetime().date(),
                #     equity.symbol,
                #     "RSI",
                #     p_change,
                # )
            context.pnl += pnl
            del context.position_dates[equity]


def screen_and_rank(context, data):

    open_ = pipeline_output("pipe")["open"]

    max_price = context.account.settled_cash * context.max_concentration
    open_ = open_[open_ <= max_price]
    historical_opens = data.history(open_.index, "open", 30, "1d")
    ema5 = historical_opens.apply(lambda col: talib.EMA(col, timeperiod=5))
    ema10 = historical_opens.apply(lambda col: talib.EMA(col, timeperiod=10))

    PriceData = namedtuple("PriceData", ["key", "price", "ema5", "ema10"])

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
    sorted_below = sorted(
        rank_filter, key=lambda x: (x.ema5 - x.price) / x.price, reverse=True
    )
    open_order_value = 0

    for name, price, ema5, ema10 in (
        (x.key, x.price, x.ema5, x.ema10) for x in sorted_below
    ):
        if (
            name not in context.position_dates
            and data.can_trade(name)
            and open_order_value < context.account.settled_cash
        ):
            order_id = order_target_percent(name, context.max_concentration)
            order = context.get_order(order_id)
            if order:
                open_order_value += order.amount * price
                context.position_dates[name] = (get_datetime(), order.amount * price)

    avg_win = context.cumm_win / context.wins if context.wins else 0
    avg_loss = context.cumm_loss / context.losses if context.losses else 0
    win_rate = round((context.wins / ((context.wins + context.losses) or 1)) * 100, 2)
    print(
        f"{get_datetime()} win rate: {win_rate} (wins(avg)/losses(avg)): ({context.wins}({round(avg_win, 2)})/{context.losses}({round(avg_loss, 2)})) pnl: {round(context.pnl, 2)}"
    )


def rebalance_start(context, data):

    evaluate_current_positions(context, data)
    screen_and_rank(context, data)
