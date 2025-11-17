import pandas as pd
import os

data = 'datasets - parquet\DB\dfNE - Balanco_Dessem_Unificado.parquet'
df = pd.read_parquet(f'{data}', engine='pyarrow')

print(df.isnull().sum())

# # print(df.isna().sum())
# # print(df.dtypes)

# col_exc = ['Data', 'DataVersao']
# col_str = ['Sistema', 'Regiao', 'Classe', 'TipoConsumidor']

# # Exclusão das colunas que apresentam valores NA ou que são desncessários
# #dfAlt = df.drop(columns= col_exc, axis=1)

# # Conversão para os tipos de dados corretos ----------------------

# #Correção na formatação dos valores
# df['Consumo'] = (df['Consumo'].astype(str)
#     .str.replace('.', '', regex=False)      # remove separador de milhar
#     .str.replace(',', '.', regex=False)     # troca vírgula por ponto
# )

# df['Consumo'] = pd.to_numeric(df['Consumo'], errors='coerce') # Para real
# df['DataExcel'] = pd.to_datetime(df['DataExcel'], errors='coerce') # Para data
# df[col_str] = df[col_str].convert_dtypes()

# #dfAlt = dfAlt.dropna(subset=['Consumo'], how='any')

# # Filtrar só a região nordeste
# dfNE = df[df['Regiao'] == 'Nordeste']
# # print(dfNE.dtypes)

# # Converter o arquivo
# nome = os.path.basename(f'{data}')
# dfNE.to_parquet(f'datasets - parquet\DB\dfNE - {nome}', engine='pyarrow', index=True)
# print('Arquivo Salvo')

# # print(dfNE)
# # print(dfNE.dtypes)
# # print(dfNE.isna().sum())