import pandas as pd

# # df = pd.read_parquet('datasets - parquet\datasets - Balanco_Dessem_Detalhe-2025-05-23.parquet')

# # print(df[df['cod_subsistema'] == "NE"].head())

lista_path = ['datasets - parquet\datasets para conversão\SETOR_INDUSTRIAL_POR_RG.csv','datasets - parquet\datasets para conversão\SETOR_INDUSTRIAL_POR_UF.csv']

for path in lista_path:

    df = pd.read_csv(f'{path}')

    newPath = path.replace('.csv','.parquet')

    df.to_parquet(f'{newPath}')
