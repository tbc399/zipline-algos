"""
Ideas:
 - in if opens higher than previous open and close
 - in if opens higher then previous close
 - in if opens higher than close and uptrend of last two bars
 - mean reversion: hold for a day. rank on opening based on how far away from the mean it is in an uptrend
 - mean reversion: determine the mean with something like a 5 day EMA on market_close the open (could also look at the closing bar)
    - could get out when it reached the "mean" or maybe when 2 period RSI breaks over threshold
 - mean reversion: using a 2-ish period rsi
 - mean reversion: for any MR strategy set something like a 100 EMA as a guard
"""
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


def T500US():
    tradables = my_default_us_equity_mask()
    return AverageDollarVolume(
        window_length=200,
        mask=tradables
    ).top(1500)


def initialize(context: TradingAlgorithm):
    
    context.wins = 0
    context.losses = 0
    
    #  initialize the context
    context.number_of_stocks = 50
    
    context.names_to_buy = None
    
    context.ma_period = 26
    
    set_commission(commission.PerTrade(cost=0.0))
    
    schedule_function(
        rebalance_start,
        date_rules.every_day(),
        time_rules.market_close(minutes=5)
    )
    
    schedule_function(
        rebalance_end,
        date_rules.every_day(),
        time_rules.market_open()
    )

    attach_pipeline(make_pipeline(context), 'pipe')
    

def make_pipeline(context: TradingAlgorithm):
    
    base_universe = T500US()
    
    ma = SimpleMovingAverage(
        inputs=[USEquityPricing.close],
        window_length=context.ma_period,
        mask=base_universe
    )
    
    max_price = context.account.settled_cash / context.number_of_stocks
    
    pipe = Pipeline(
        screen=(
            base_universe #&
            #(USEquityPricing.close.latest > ma) &
            #(USEquityPricing.close.latest < ma) &
            #(USEquityPricing.close.latest <= max_price)
        ),
        columns={
            'close': USEquityPricing.close.latest
        }
    )
    
    return pipe


def rebalance_start(context: TradingAlgorithm, data: BarData):
    print(get_datetime())
    
    close = pipeline_output('pipe')['close']
    max_price = context.account.settled_cash / context.number_of_stocks
    #close = close[close <= max_price]

    #closing_prices = data.history(close.index, "close", 2, "1d")

    names_to_buy = close.sample(min(context.number_of_stocks, len(close)))
    pos_size = 1.0 / context.number_of_stocks

    for name, price in names_to_buy.items():
        if data.can_trade(name):# and price > closing_prices[name][0]:
            order_target_percent(name, pos_size)


def rebalance_end(context: TradingAlgorithm, data: BarData):
    
    for name in context.portfolio.positions.keys():
        if data.can_trade(name):
            order_target_percent(name, 0)