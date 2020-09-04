import pandas as pd
import os

df = pd.read_csv(os.getcwd() + '/test.csv', delimiter=";", encoding='iso-8859-1')
ls = df['ISBN'].tolist()
for val in ls:
    print(val, type(val))
    s = str(val)
    print(s, type(s))