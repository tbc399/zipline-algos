import sys
import pandas as pd
from plotly import express


df = pd.read_pickle(f'{sys.argv[1]}')
algo_returns = df.algorithm_period_return
bench_returns = df.benchmark_period_return
fig = express.line(pd.concat([algo_returns, bench_returns], axis=1))
fig.show()

