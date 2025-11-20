import pandas as pd
import os

lista = os.listdir('datasets - parquet\DB')

unidata = []
for data in lista:
    if data.startswith('dfNE - CurvaCarga'):
        df = pd.read_parquet(f'datasets - parquet\DB\{data}')
        unidata.append(df)

dfFinal = pd.concat(unidata, ignore_index=True)

dfFinal.to_parquet(f'datasets - parquet\DB\dfNE - CurvaCarga_Unificado.parquet', engine='pyarrow', index=True)
print('Arquivo Salvo')