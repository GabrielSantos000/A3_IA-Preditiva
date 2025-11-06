import pandas as pd
import os

df1 = pd.read_csv('datasets - parquet\consumo_energia_nordeste.csv')
df1.to_parquet('seu_arquivo.parquet', engine='pyarrow')
