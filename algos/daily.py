"""
Ideas:
 - in if opens higher than previous open and close
 - in if opens higher then previous close
 - in if opens higher than close and uptrend of last two bars
 - mean reversion: hold for a day. rank on opening based on how far away from the mean it is in an uptrend
    - would need large swings from the mean
 - mean reversion: determine the mean with something like a 5 day EMA on market_close the open (could also look at the closing bar)
    - could get out when it reached the "mean" or maybe when 2 period RSI breaks over threshold
 - mean reversion: using a 2-ish period rsi
 - mean reversion: for any MR strategy set something like a 100 EMA as a guard
"""
import talib
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
    get_datetime
)
from zipline.finance import commission
from zipline.utils.events import date_rules, time_rules
from zipline.protocol import BarData, Account
from zipline.pipeline import Pipeline
from zipline.pipeline.data.equity_pricing import USEquityPricing
from zipline.pipeline.factors import (
    AverageDollarVolume,
    SimpleMovingAverage
)


def my_default_us_equity_mask():
    has_prev_close = USEquityPricing.close.latest.notnull()
    has_prev_vol = USEquityPricing.volume.latest > 0
    return has_prev_close & has_prev_vol


def T1000US():
    tradables = my_default_us_equity_mask()
    return AverageDollarVolume(
        window_length=200,
        mask=tradables
    ).top(1000)


def initialize(context: TradingAlgorithm):
    
    context.wins = 0
    context.losses = 0
    #  initialize the context
    context.max_concentration = .04
    context.names_to_buy = None
    
    set_commission(commission.PerTrade(cost=0.0))
    
    schedule_function(
        rebalance_start,
        date_rules.every_day(),
        time_rules.market_open(minutes=1)
    )
    
    #schedule_function(
    #    rebalance_end,
    #    date_rules.every_day(),
    #    time_rules.market_open()
    #)

    attach_pipeline(make_pipeline(context), 'pipe')
    

def make_pipeline(context: TradingAlgorithm):
    
    base_universe = T1000US()
    
    pipe = Pipeline(
        screen=base_universe,
        columns={'open': USEquityPricing.open.latest}
    )
    
    return pipe


def rebalance_start(context: TradingAlgorithm, data: BarData):
    print(get_datetime())

    open_ = pipeline_output('pipe')['open']
    
    for equity, position in context.portfolio.positions.items():
        # check time bound exit, ex 5 days
        historical_opens = data.history(open_.index, "open", 10, "1d")
        ema = historical_opens.apply(lambda col: talib.EMA(col, timeperiod=5))
        rsi

    max_price = context.account.settled_cash * context.max_concentration
    open_ = open_[open_ <= max_price]
    historical_opens = data.history(open_.index, "open", 10, "1d")
    ema = historical_opens.apply(lambda col: talib.EMA(col, timeperiod=5))
    
    combo = [(key, historical_opens.get(key).iloc[-1], ema.get(key).iloc[-1]) for key in historical_opens.keys()]
    below = [x for x in combo if x[1] < x[2]]
    sorted_below = sorted(below, key=lambda x: (x[2] - x[1]) / x[1], reverse=True)
    open_order_value = 0
    

    for name in (x[0] for x in sorted_below):
        if data.can_trade(name) and open_order_value < context.account.settled_cash:
            order_id = order_target_percent(name, context.max_concentration)
            open_order_value += context.get_order(order_id).amount


def rebalance_end(context: TradingAlgorithm, data: BarData):
    
    for name in context.portfolio.positions.keys():
        if data.can_trade(name):
            order_target_percent(name, 0)
