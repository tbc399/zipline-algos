from zipline.api import (
    set_commission,
    symbol,
    schedule_function,
    record,
    attach_pipeline,
    pipeline_output,
    order_target_percent,
    get_open_orders,
)
from zipline.finance import commission
from zipline.utils.events import date_rules, time_rules
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


def initialize(context):
    
    #  initialize the context
    context.number_of_stocks = 25
    
    context.names_to_buy = None
    
    context.long_ema_length = 26
    
    set_commission(commission.PerTrade(cost=0.0))
    
    schedule_function(
        rebalance_start,
        date_rules.every_day(),
        time_rules.market_open()
    )
    
    schedule_function(
        rebalance_end,
        date_rules.every_day(),
        time_rules.market_close(minutes=5)
    )
    '''
    schedule_function(
        record_vars,
        date_rules.every_day(),
        time_rules.market_close(),
    )
    '''

    attach_pipeline(make_pipeline(context), 'pipe')


def months_to_days(months):
    return 21 * months


def make_pipeline(context):
    
    # base_universe = Q500US()
    base_universe = T500US()
    
    long_ema = SimpleMovingAverage(
        inputs=[USEquityPricing.close],
        window_length=context.long_ema_length,
        mask=base_universe
    )
    
    pipe = Pipeline(
        screen=(
            base_universe
            #(USEquityPricing.close.latest < long_ema) &
            #(USEquityPricing.close.latest <= 50)
        ),
        columns={
            'close': USEquityPricing.close.latest#,
            #'ema': long_ema
        }
    )
    
    return pipe


def rebalance_start(context, data):
    
    # todo: try filtering on open greater than previous open
    close = pipeline_output('pipe')['close']
    prices = data.history(pipeline_output('pipe').index, "open", 180, "1d")

    names_to_buy = close.sample(context.number_of_stocks)
    pos_size = 1.0 / context.number_of_stocks

    for name in names_to_buy.keys():
        if data.can_trade(name):
            order_target_percent(name, pos_size)


def rebalance_end(context, data):
    
    for name in context.portfolio.positions.keys():
        if data.can_trade(name):
            order_target_percent(name, 0)


def record_vars(context, data):
    aapl = pipeline_output('pipe').loc[symbol('AAPL')]
    record(ema=aapl['ema'])
    record(close=aapl['close'])
