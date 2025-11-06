import pandas as pd
import os

# df1 = pd.read_csv('datasets - parquet\consumo_energia_nordeste.csv')
# df1.to_parquet('seu_arquivo.parquet', engine='pyarrow')


# print(df_test.dtypes)
# print(df_test.head(15))

# Caminho dos datasets 
pathData = 'datasets - parquet'
pathData_processado = 'DB'

# Listagens dos datasets
listaDatasets = os.listdir(pathData)
Datasets_processados = os.listdir(pathData_processado)

# processando = []

# for data in listaDatasets:
#     if data.startswith('datasets - Balanco'):
#         processando.append(data)
#         df = pd.read_parquet(f'{data}')

df = pd.read_parquet('datasets - parquet\datasets - Balanco_Dessem_Detalhe-2025-05-23.parquet')

df['val_ger_fotovoltaica'] = pd.to_numeric(df['val_ger_fotovoltaica'], errors='coerce')
df['val_ger_mmgd'] = pd.to_numeric(df['val_ger_mmgd'], errors='coerce')
df['val_cons_elevatoria'] = pd.to_numeric(df['val_cons_elevatoria'], errors='coerce')

df.drop(labels='val_ger_fotovoltaica', axis=1)
print(df.head())


