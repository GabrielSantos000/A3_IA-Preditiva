import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np

#data = "datasets - parquet\datasets_tratados_unificados\dfNE - SETOR_INDUSTRIAL_POR_UF.parquet"

# df = pd.read_parquet(f'{data}', engine='pyarrow')

# def grafico(df, figsize=(10,5), salvar=True, prefixo="gráfcio -"):
#     """
#     Gera gráficos de dispersão para todas as colunas numéricas de um dataset.
#     Cada gráfico mostra: índice (x) vs valor da coluna (y).

#     Parâmetros:
#         df (pd.DataFrame): dataset de entrada
#         figsize (tuple): tamanho da figura
#         salvar (bool): salvar imagens em PNG
#         prefixo (str): prefixo dos arquivos
#     """
#     col_num = df.select_dtypes(include='number').columns

#     for col in col_num:
#         plt.figure(figsize=figsize)
#         plt.scatter(df.index, df[col])
#         plt.title(f"Gráfico de Dispersão — {col}")
#         plt.xlabel("Índice")
#         plt.ylabel(col)

#         if salvar:
#             plt.savefig(f"{prefixo}{col}.png", dpi=200)
        
#         plt.show()

lista = os.listdir('datasets - parquet\datasets_tratados_unificados')

for data in lista:

    path = f'datasets - parquet\datasets_tratados_unificados\{data}'
    df = pd.read_parquet(path, engine='pyarrow')
    #grafico(df)


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
    
    print(data)
    print(df)
    print(df.dtypes)
