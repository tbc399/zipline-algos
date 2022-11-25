import statistics
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
fig.show()


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

    def __str__(self):
        return f'{self.buy_dt.date()} {self.sell_dt.date()} {self.name} {self.pnl}'


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


wins = [trade for trade in trades if trade.pnl > 0]
losses = [trade for trade in trades if trade.pnl < 0]

print(f'Wins: {len(wins)}')
print(f'Losses: {len(losses)}')

avg_win = sum([win.pnl for win in wins])
avg_loss = sum([loss.pnl for loss in losses])

print(f'Avg win: {round(avg_win / len(wins), 2)}')
print(f'Avg loss: {round(avg_loss / len(losses), 2)}')

print(f'Stddev win: {round(statistics.stdev((win.pnl for win in wins)), 2)}')
print(f'Stddev loss: {round(statistics.stdev((loss.pnl for loss in losses)), 2)}')

win_hold_times = [(win.sell_dt - win.buy_dt).days for win in wins]
loss_hold_times = [(loss.sell_dt - loss.buy_dt).days for loss in losses]

avg_win_hold_time = round(sum(win_hold_times) / len(win_hold_times))
stddev_win_hold_time = round(statistics.stdev(win_hold_times))
mean_win_hold_time = round(statistics.mean(win_hold_times))
print(
    f'Win hold time: avg({avg_win_hold_time}) stdev({stddev_win_hold_time}) mean({mean_win_hold_time})'
)

avg_loss_hold_time = round(sum(loss_hold_times) / len(loss_hold_times))
stddev_loss_hold_time = round(statistics.stdev(loss_hold_times))
mean_loss_hold_time = round(statistics.mean(loss_hold_times))
print(
    f'Loss hold time: avg({avg_loss_hold_time}) stdev({stddev_loss_hold_time}) mean({mean_loss_hold_time})'
)

# for trade in trades[:200]:
for trade in trades[400:800]:
    print(trade)
