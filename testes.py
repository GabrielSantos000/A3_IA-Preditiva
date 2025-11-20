import pandas as pd
import os

# data = "datasets - parquet\datasets originais\DB-processando\CONSUMO_E_NUMCONS_SAM_UF.parquet"

# df = pd.read_parquet(f'{data}', engine='pyarrow')

# col_str = ['UF','Regiao', 'Sistema', 'Classe', 'TipoConsumidor']
# col_exc = ['Data','DataVersao']

# df[col_str] = df[col_str].convert_dtypes()
# df['DataExcel'] = pd.to_datetime(df['DataExcel'], errors='coerce')
# # df['Consumo'] = pd.to_numeric(df['Consumo'], errors='coerce')

# # df["Consumo"] = (
# #     df["Consumo"]
# #     .astype(str).str.replace(".", "", regex=False)
# #     .astype("float64")
# # )

# def limpar_pontos(valor):
#     valor = str(valor)

#     # Se não houver ponto ou houver apenas um, não precisa mexer
#     if valor.count('.') <= 1:
#         return valor

#     # Se houver vários pontos:
#     ## dividir em partes
#     partes = valor.split('.')
    
#     ## manter a última parte (decimal)
#     decimal = partes[-1]
    
#     ## juntar todas as partes anteriores sem ponto
#     inteiro = ''.join(partes[:-1])

#     return inteiro + "." + decimal

# df["Consumo"] = df["Consumo"].apply(limpar_pontos)
# df["Consumo"] = df["Consumo"].astype(float)

# df2 = df.drop(columns=col_exc, axis=1)

# print(df2.isna().sum())
# print(df2.dtypes)
# print(df2)

# # # # df1 = df.dropna()

# # # # # # print(pd.notna(df1).sum())

# df2.to_parquet('datasets - parquet\datasets_tratados_unificados\dfNE - CONSUMO_E_NUMCONS_SAM_UF.parquet', engine='pyarrow', index=True)
# print('Arquivo Salvo')

lista = os.listdir('datasets - parquet\datasets_tratados_unificados')

for data in lista:

    path = f'datasets - parquet\datasets_tratados_unificados\{data}'
    df = pd.read_parquet(path, engine='pyarrow')
    
    print(data)
    print(df)
    print(df.dtypes)
