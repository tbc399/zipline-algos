from algos import smooth_momentum as algo
from live import live_base


def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    algo.initialize(context)
    live_base.initialize(context)
