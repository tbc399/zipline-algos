import statistics
import math
from typing import List
from dataclasses import dataclass
from datetime import datetime
import sys
import pandas as pd
from plotly import express

from yfinance import download


def percent_change(start, finish):
    return (finish - start) / start


df = pd.read_pickle(f"{sys.argv[1]}")
algo_returns = pd.Series(
    df.algorithm_period_return.values, df.algorithm_period_return.index.date
)


def plot():
    # benchmark_symbol = '^SPX'
    benchmark_symbol = 'SPY'
    spx = download(
        [benchmark_symbol],
        start=df.algorithm_period_return.index[0].date(),
        end=df.algorithm_period_return.index[-1].date(),
    ).drop(columns=['Open', "Close", 'High', 'Low', 'Volume'])

    spx_returns = [
        (date.date(), percent_change(spx.values[0][0], value[0]))
        for date, value in spx.iterrows()
    ]

    bench_returns = pd.DataFrame(
        spx_returns, columns=['Date', benchmark_symbol]
    ).set_index('Date')

    fig = express.line(
        # pd.concat([algo_returns, bench_returns], axis=1), title=sys.argv[1].split()[0]
        algo_returns,
        title=sys.argv[1].split()[0],
    )
    fig.show()


plot()


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
    buy_price: float
    sell_price: float
    name: str
    pnl: float

    def __str__(self):
        return f'{self.buy_dt.date()} {self.sell_dt.date()} {self.name} {self.pnl}%'


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
                buy_price=buy_price,
                sell_price=sell_price,
                name=transaction.sid.symbol,
                pnl=round(((sell_price - buy_price) / buy_price) * 100, 2),
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

print(f'Avg win: {round(avg_win / len(wins), 2)}%')
print(f'Avg loss: {round(avg_loss / len(losses), 2)}%')

print(f'Stddev win: {round(statistics.stdev((win.pnl for win in wins)), 2)}%')
print(f'Stddev loss: {round(statistics.stdev((loss.pnl for loss in losses)), 2)}%')

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
years = math.ceil((df.starting_value.index[-1] - df.starting_value.index[0]).days / 365)
algo_cagr = ((df.ending_value[-1] / df.starting_cash[0]) ** (1 / years) - 1) * 100
print(f'CAGR: {round(algo_cagr, 2)}')
print(f'Sharpe: {round(df.sharpe[-1], 2)}')
print("Ending Account Value: ${:,.2f}".format(df.ending_value[-1]))

# for trade in trades[:200]:
# for trade in trades[400:800]:
#     print(trade)
