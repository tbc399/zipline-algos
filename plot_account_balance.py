import sys
import pandas as pd
from plotly import express


df = pd.read_pickle(f'{sys.argv[1]}')
account_value = df.ending_value
fig = express.line(account_value, title=sys.argv[1].split()[0])
fig.show()
