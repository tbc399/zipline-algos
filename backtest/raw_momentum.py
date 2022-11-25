from algos import raw_momentum as algo

from zipline.finance import commission
from zipline.api import set_commission


def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    algo.initialize(context)
    set_commission(commission.PerTrade(0.0))


def handle_data(context, data):
    open_orders = get_open_orders().values()
    open_sell_orders = [
        x for sub_list in open_orders for x in sub_list if x.amount == 0
    ]

    if not len(open_sell_orders) and context.names_to_buy:

        pos_size = 1.0 / context.number_of_stocks

        for name in context.names_to_buy:
            try:
                order_target_percent(name, pos_size)
            except CannotOrderDelistedAsset as error:
                print(error)

        context.names_to_buy = None
