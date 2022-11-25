from zipline.api import (
    set_commission,
    schedule_function,
    attach_pipeline,
    sid,
    pipeline_output,
    order_target_percent,
)
from zipline.utils.events import date_rules, time_rules
from zipline.finance import commission
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import AverageDollarVolume


def initialize(context):

    attach_pipeline(make_pipeline(), 'pipeline')

    set_commission(commission.PerTrade(cost=0.0))

    schedule_function(
        trade, date_rules.month_end(), time_rules.market_close(minutes=30)
    )

    context.spy = sid(8554)
    context.tf_filter = False
    context.tf_lookback = 126

    context.target_securities_to_buy = 30.0
    # context.bonds = sid(23870)

    # Other parameters
    context.top_n_roe_to_buy = 50  # First sort by ROE
    context.relative_momentum_lookback = 126  # Momentum lookback
    context.momentum_skip_days = 20
    context.top_n_relative_momentum_to_buy = 30  # Number to buy


def mask():

    has_prev_close = USEquityPricing.close.latest.notnull()
    has_prev_vol = USEquityPricing.volume.latest > 0

    return has_prev_close & has_prev_vol


def Q1500US():

    return AverageDollarVolume(window_length=200, mask=mask()).top(500)


def make_pipeline():

    universe = Q1500US()

    # roe = Fundamentals.roe.latest
    # sma = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=100)

    pipe = Pipeline(
        # columns={'roe': roe},
        screen=universe  # & (USEquityPricing.close.latest > sma)
    )

    return pipe


def before_trading_start(context, data):

    #  TODO: the documentation says not to do this, so need to remove it
    # context.output = algo.pipeline_output('pipeline')
    # context.security_list = context.output.index
    context.security_list = pipeline_output('pipeline').index


def trade(context, data):

    ############Trend Following Regime Filter############
    tf_hist = data.history(context.spy, "close", 140, "1d")
    tf_check = tf_hist.pct_change(context.tf_lookback).iloc[-1]

    if tf_check > 0.0:
        context.tf_filter = True
    else:
        context.tf_filter = False
    ############Trend Following Regime Filter End############

    # DataFrame of Prices for our 500 stocks
    prices = data.history(context.security_list, "close", 180, "1d")
    # DF here is the output of our pipeline, contains 500 rows (for 500 stocks) and one column - ROE
    # df = pipeline_output('pipeline')

    # Grab top 50 stocks with best ROE
    # top_n_roe = df['roe'].nlargest(context.top_n_roe_to_buy)
    # Calculate the momentum of our top ROE stocks
    # quality_momentum = prices[top_n_roe.index][:-context.momentum_skip_days].pct_change(context.relative_momentum_lookback).iloc[-1]
    quality_momentum = (
        prices[: -context.momentum_skip_days]
        .pct_change(context.relative_momentum_lookback)
        .iloc[-1]
    )
    # Grab stocks with best momentum
    top_n_by_momentum = quality_momentum.nlargest(
        context.top_n_relative_momentum_to_buy
    )

    for x in context.portfolio.positions:
        if x not in top_n_by_momentum:
            order_target_percent(x, 0)

    for x in top_n_by_momentum.index:
        if x not in context.portfolio.positions and context.tf_filter:
            order_target_percent(x, (1.0 / context.target_securities_to_buy))


def get_benchmark(symbol=None, start=None, end=None):
    bm = yahoo_reader.DataReader(
        symbol, 'yahoo', pd.Timestamp(start), pd.Timestamp(end)
    )['Close']
    bm.index = bm.index.tz_localize('UTC')
    return bm.pct_change(periods=1).fillna(0)
