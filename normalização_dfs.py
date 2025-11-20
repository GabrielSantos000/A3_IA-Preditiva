import pandas as pd
from sklearn.preprocessing import MinMaxScaler

# Carregar seu dataset unificado
dataset = 'datasets - parquet\datasets_tratados_unificados\dfNE - Balanco_Dessem_Unificado.parquet'
df = pd.read_parquet(f"{dataset}", engine='pyarrow')

# Selecionar apenas colunas numéricas (importante!)
cols_numericas = df.select_dtypes(include=['float64', 'int64']).columns

# Criar o scaler
scaler = MinMaxScaler()

# Ajustar e transformar os dados
df[cols_numericas] = scaler.fit_transform(df[cols_numericas])

print(df)

# Salvar o resultado
# df.to_parquet(f'{data}', engine='pyarrow', index=True)
# print('Arquivo Salvo')
