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
    sid,
    order_target_percent,
    get_open_orders,
    record,
    get_datetime
)
from zipline.finance import commission
from zipline.utils.events import date_rules, time_rules
from zipline.pipeline import Pipeline, CustomFactor
from zipline.pipeline.data.equity_pricing import EquityPricing
from zipline.pipeline.factors import AverageDollarVolume
from zipline.errors import CannotOrderDelistedAsset
'''
from zipline.pipeline.filters.fundamentals import (
    IsPrimaryShare,
    IsDepositaryReceipt,
    is_common_stock
)
'''


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
    
    #  the rebalance frequency in months
    context.rebalance_freq = 1
    
    #  window length for evaluating momentum in months
    context.window_length = 6
    
    #  need to keep track of sell orders
    context.sell_orders = set()
    
    context.names_to_buy = None
    
    context.tf_lookback = months_to_days(6)
    
    set_commission(commission.PerTrade(cost=0.0))
    
    schedule_function(
        rebalance,
        date_rules.month_start(),
        time_rules.market_open()
    )
    
    attach_pipeline(make_pipeline(context), 'pipe')


def months_to_days(months):
    
    return 21 * months


def make_pipeline(context):
    """TODO"""

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
                
    
def rebalance(context, data):
    """Rebalance every month"""
    
    '''
    hist = data.history(sid(8554), "close", 140, "1d")
    check = hist.pct_change(context.tf_lookback).iloc[-1]

    if check > 0.0:
        context.tf_filter = True
    else:
        context.tf_filter = False
    '''
        
    if get_datetime().month not in range(1, 13, context.rebalance_freq):
        return
    
    context.output = pipeline_output('pipe')
    
    returns = context.output['returns']
    #assets = pipeline_output('pipe').index
    '''
    returns = data.history(
        assets,
        'close',
        months_to_days(context.window_length),
        '1d'
    ).pct_change(months_to_days(context.window_length)).iloc[-1]
    print(returns)
    #quality = r_value_quality(assets, context.window_length, data)
    '''
    '''
    quality = r_value_quality(
        returns.nlargest(100).index,
        #returns.nsmallest(100).index,
        context.window_length,
        data
    )
    '''
    '''
    #print(type(returns))
    #print(returns.nlargest(10).index)
    #print(returns.nsmallest(10).index)
    #quality = context.output['quality']
    top_ranked_names = long_rank(returns, None)[:context.number_of_stocks_for_selection]
    top_ranked_names = [
        x for x in long_rank(returns, quality)[:context.number_of_stocks_for_selection]
        if re.match(r'^[A-Z]+$', x.symbol)
    ]
    #print(quality)
    top_ranked_names = [
        x for x in quality.sort_values(ascending=False)[:context.number_of_stocks_for_selection].keys()
        if re.match(r'^[A-Z]+$', x.symbol)
    ]
    '''

    #print(pandas.concat((returns, quality), axis=1))
    
    #print(','.join([x.symbol for x in top_ranked_names[:context.number_of_stocks]]))
    
    '''
    top_long_names = set(
        top_ranked_names[:context.number_of_stocks]
    )
    '''
    #print(len(quality))
    #top_long_names = set(quality.nlargest(context.number_of_stocks).keys())
    top_long_names = set(returns.nlargest(context.number_of_stocks).keys()) if not returns.empty else set()
    #print(get_datetime())
    #print(returns)
    #names_with_returns = [(x.symbol, [y for y in returns if y.symbol == x.symbol][0]) for x in top_long_names]
    #for x, y in names_with_returns:
    #    print(x, y)
    #print('============================')
    #print(get_datetime(), '|', ','.join(x.symbol for x in top_long_names))
    current_names = set(context.portfolio.positions.keys())
    
    context.names_to_buy = top_long_names - current_names
    #print(get_datetime(), '|', ','.join(x.symbol for x in context.names_to_buy))
    names_to_sell = current_names - top_long_names
        
    for name in names_to_sell:
        
        if data.can_trade(name):
            order_target_percent(name, 0)
        
    #position_size = 1.0 / context.number_of_stocks
    
    #for name in names_to_buy:
        
    #    if data.can_trade(name):
    #        order_target_percent(name, position_size)
    

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

            
def record_vars(context, data):
    record(account_value=context.account.net_liquidation)