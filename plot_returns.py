from collections import defaultdict
from typing import List
from dataclasses import dataclass
from datetime import datetime
import sys
import pandas as pd
from plotly import express


df = pd.read_pickle(f"{sys.argv[1]}")
algo_returns = df.algorithm_period_return
bench_returns = df.benchmark_period_return
fig = express.line(
    pd.concat([algo_returns, bench_returns], axis=1), title=sys.argv[1].split()[0]
)
#fig.show()


@dataclass
class Transaction:
    amount: int
    dt: datetime
    price: float
    order_id: str
    sid: object
    commission: None


transactions = []
for date in df.transactions:
    for item in date:
        transactions.append(Transaction(**item))


@dataclass
class Trade:
    buy_dt: datetime
    sell_dt: datetime
    name: str
    pnl: float
    
    
trade_pairings = dict()
trades: List[Trade] = []
for transaction in transactions:
    buy_transaction = trade_pairings.get(transaction.sid)
    if buy_transaction:
        buy_price = buy_transaction.amount * buy_transaction.price
        sell_price = abs(transaction.price * transaction.amount)
        trades.append(
            Trade(
                buy_dt=buy_transaction.dt.tz_convert('US/Eastern').to_pydatetime(),
                sell_dt=transaction.dt.tz_convert('US/Eastern').to_pydatetime(),
                name=transaction.sid.symbol,
                pnl=round(sell_price - buy_price, 2),
            )
        )
        del trade_pairings[transaction.sid]
    else:
        trade_pairings[transaction.sid] = transaction

for trade in trades[:50]:
    print(trade)
