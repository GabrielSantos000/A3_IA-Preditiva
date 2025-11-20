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

from sklearn.model_selection import train_test_split

# 1. Separar variável alvo
var_principal = " "
X = df.drop(columns=[var_principal])
y = df[var_principal]

# 2. Dividir treino e teste (antes de normalizar!)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, shuffle=False
)

# 3. Criar o scaler
scaler = MinMaxScaler()

# 4. Ajustar + normalizar apenas o treino
X_train_scaled = scaler.fit_transform(X_train)

# 5. Normalizar o teste usando o mesmo scaler
X_test_scaled = scaler.transform(X_test)
