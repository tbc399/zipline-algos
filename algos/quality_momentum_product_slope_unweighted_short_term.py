"""
Idea:
A shorter term momentum approach. Still using days, but shorten it to something
like a one month lookback with a one week hold time or something like that.
"""

import numpy as np
from scipy import stats

from zipline.api import (
    set_commission,
    schedule_function,
    attach_pipeline,
    pipeline_output,
    get_datetime,
    order_target_percent,
    get_open_orders,
)
from zipline.finance import commission
from zipline.utils.events import date_rules, time_rules
from zipline.pipeline import Pipeline, CustomFactor
from zipline.pipeline.data.equity_pricing import EquityPricing
from zipline.pipeline.factors import AverageDollarVolume
from zipline.errors import CannotOrderDelistedAsset


class MomentumQuality(CustomFactor):
    
    inputs = [EquityPricing.close]

    def compute(self, today, assets, out, close):
        
        prices = close.transpose()
        x = np.arange(self.window_length)
        output = []
        for col in prices:  
            slope, _, r_value, _, _ = stats.linregress(x, col)
            output.append(slope * r_value ** 2)

        out[:] = output


def my_default_us_equity_mask():
    
    has_prev_close = EquityPricing.close.latest.notnull()
    has_prev_vol = EquityPricing.volume.latest > 0
    
    return has_prev_close & has_prev_vol
    

def _tus(limit):
    tradables = my_default_us_equity_mask()
    return AverageDollarVolume(
        window_length=200,
        mask=tradables
    ).top(limit)


def T500US():
    return _tus(500)


def T1000US():
    return _tus(1000)


def initialize(context):
    """
    Called once at the start of the algorithm.
    """   
    context.number_of_stocks = 50
    context.number_of_stocks_for_selection = 30
    
    #  window length for evaluating momentum in months
    context.window_length = 1
    
    #  need to keep track of sell orders
    context.sell_orders = set()
    
    context.names_to_buy = None
    
    set_commission(commission.PerTrade(cost=0.0))
    
    schedule_function(
        rebalance,
        date_rules.week_start(),
        time_rules.market_open()
    )
    
    attach_pipeline(make_pipeline(context), 'pipe')


def months_to_days(months):
    
    return 21 * months


def make_pipeline(context):

    base_universe = T500US()

    quality_returns = MomentumQuality(
        inputs=[EquityPricing.close],
        window_length=months_to_days(context.window_length),
        mask=base_universe
    )
    
    pipe = Pipeline(
        screen=base_universe & (quality_returns > 0),
        columns={
            'returns': quality_returns,
        }
    )
    
    return pipe


def rebalance(context, data):
    print(get_datetime().date())
    
    context.output = pipeline_output('pipe')
    
    returns = context.output['returns']
    top_long_names = set(returns.nlargest(context.number_of_stocks).keys()) if not returns.empty else set()
    current_names = set(context.portfolio.positions.keys())
    
    context.names_to_buy = top_long_names - current_names
    names_to_sell = current_names - top_long_names
        
    for name in names_to_sell:
        
        if data.can_trade(name):
            order_target_percent(name, 0)

def handle_data(context, data):
    
    open_orders = get_open_orders().values()
    open_sell_orders = [
        x for sub_list in open_orders for x in sub_list if x.amount == 0]
    
    if not len(open_sell_orders) and context.names_to_buy:
        
        pos_size = 1.0 / context.number_of_stocks
        
        for name in context.names_to_buy:
            try:
                order_target_percent(name, pos_size)
            except CannotOrderDelistedAsset as error:
                print(error)
            
        context.names_to_buy = None
