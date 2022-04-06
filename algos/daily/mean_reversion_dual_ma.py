"""
Idea:
- determine the mean with something like a 5 day EMA on the open
- determine the trend with a 10 day EMA to compare with the 5 day EMA
- Look for opening at or below both MAs in up trend
- Look for an RSI that is still above the oversold threshold
- could get out when it reached the "mean" or maybe when 2 period RSI breaks over threshold
- rank on biggest move from EMA (slow or fast)?
- rank on biggest diff between fast and slow EMA
"""
import talib
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
from zipline.protocol import BarData, Account
from zipline.pipeline import Pipeline
from zipline.pipeline.data.equity_pricing import USEquityPricing
from zipline.pipeline.factors import AverageDollarVolume, SimpleMovingAverage


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
    context.losses = 0
    context.rsi_exits = 0
    context.ema_exits = 0
    context.time_exits = 0
    #  initialize the context
    context.max_concentration = 0.04
    context.names_to_buy = None
    context.position_dates = {}

    set_commission(commission.PerTrade(cost=0.0))

    schedule_function(
        rebalance_start, date_rules.every_day(), time_rules.market_open(minutes=1)
    )

    attach_pipeline(make_pipeline(context), "pipe")


def make_pipeline(context: TradingAlgorithm):

    # base_universe = T500US()
    base_universe = T1000US()

    pipe = Pipeline(screen=base_universe, columns={"open": USEquityPricing.open.latest})

    return pipe


def rebalance_start(context: TradingAlgorithm, data: BarData):
    #open_ = pipeline_output("pipe")["open"]

    for equity, position in context.portfolio.positions.items():
        historical_opens = data.history(equity, "open", 30, "1d")
        ema10 = talib.EMA(historical_opens, timeperiod=10)
        if context.trading_calendar.sessions_window(get_datetime().date(), -5)[0] >= context.position_dates[equity]:
            if ema10.iloc[-1] >= historical_opens.iloc[-1]:
                context.time_exits += 1
                #print(f'{get_datetime()} Time Sell')
                order_target_percent(equity, 0)
                del context.position_dates[equity]
                continue

        rsi = talib.RSI(historical_opens, timeperiod=2)
        if rsi.iloc[-1] >= 80:
            context.rsi_exits += 1
            #print(f'{get_datetime()} RSI Sell')
            order_target_percent(equity, 0)
            del context.position_dates[equity]
        # elif ema.iloc[-1] < historical_opens.iloc[-1]:
        #     context.ema_exits += 1
        #     order_target_percent(equity, 0)
        #     del context.position_dates[equity]

    max_price = context.account.settled_cash * context.max_concentration
    #open_ = open_[open_ <= max_price]
    #historical_opens = data.history(open_.index, "open", 20, "1d")
    historical_opens = data.history(symbol('WMT'), "open", 30, "1d")
    #rsi = historical_opens.apply(lambda col: talib.RSI(col, timeperiod=2))
    #ema5 = historical_opens.apply(lambda col: talib.EMA(col, timeperiod=5))
    #ema10 = historical_opens.apply(lambda col: talib.EMA(col, timeperiod=10))
    ema5 = talib.EMA(historical_opens, timeperiod=5)
    ema10 = talib.EMA(historical_opens, timeperiod=10)

    # combo = [
    #     (
    #         key,
    #         historical_opens.get(key).iloc[-1],
    #         #ema5.get(key).iloc[-1],
    #         #ema10.get(key).iloc[-1],
    #         ema5.iloc[-1],
    #         ema10.iloc[-1],
    #         #rsi.get(key).iloc[-1],
    #     )
    #     for key in historical_opens.keys()
    # ]
    combo = [
        (
            historical_opens.name,
            historical_opens.iloc[-1],
            #ema5.get(key).iloc[-1],
            #ema10.get(key).iloc[-1],
            ema5.iloc[-1],
            ema10.iloc[-1],
            #rsi.get(key).iloc[-1],
        )
    ]
    below = [x for x in combo if x[1] < x[3] < x[2]]
    sorted_below = sorted(below, key=lambda x: (x[2] - x[1]) / x[1], reverse=True)
    open_order_value = 0

    for name, price, ema5, ema10 in ((x[0], x[1], x[2], x[3]) for x in sorted_below):
        if (
            name not in context.position_dates
            and data.can_trade(name)
            and open_order_value < context.account.settled_cash
        ):
            #print(f'{get_datetime()} Buy {price} ema5={round(ema5, 2)} ema10={round(ema10, 2)}')
            order_id = order_target_percent(name, context.max_concentration)
            order = context.get_order(order_id)
            if order:
                open_order_value += order.amount * price
                context.position_dates[name] = get_datetime()

    print(
        f"exit type (Time/RSI/EMA): {context.time_exits}/{context.rsi_exits}/{context.ema_exits}"
    )
