import pandas as pd
import os

data = "datasets - parquet\datasets originais\DB-processando\CONSUMO_E_NUMCONS_SAM_UF.parquet"

df = pd.read_parquet(f'{data}', engine='pyarrow')
print(df.isna().sum())