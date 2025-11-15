import pandas as pd
import os

# Caminho dos datasets 
pathData = 'datasets - parquet\DB\DB-NE'
os.makedirs(f'{pathData}', exist_ok=True)

#------- Conversão de dados do dataset de Balanço DESSEM ---------------

# Listagens dos datasets
listaDatasets = os.listdir('datasets - parquet')

colunas_float = ['val_demanda', 'val_ger_hidraulica', 'val_ger_pch', 'val_ger_termica', 'val_ger_pct', 'val_ger_eolica']

for data in listaDatasets:
    if data.startswith('datasets - Balanco'):
        # Leitura do dataframe
        df = pd.read_parquet(f'datasets - parquet\{data}', engine='pyarrow')

        # Conversão para os tipos de dados corretos
        df[colunas_float] = df[colunas_float].apply(pd.to_numeric, errors='coerce') # Para real
        df['din_programacaodia'] = pd.to_datetime(df['din_programacaodia'], errors='coerce') # Para data

        # Exclusão das colunas que apresentam valores zerados em sua maioria
        dfAlt = df.drop(columns=['val_ger_fotovoltaica', 'val_ger_mmgd','val_cons_elevatoria'], axis=1)

        # Filtrar só a região nordeste
        df_nordeste = dfAlt[dfAlt['cod_subsistema'] == 'NE']

        # Renomar arquivo
        data_NE = data.replace('datasets','dfNE')

        # Salvar na pasta de dataframe processados
        df_nordeste.to_parquet(f'{pathData}\{data_NE}', index=False)
        print(f'{data_NE} salvo na pasta DB')


# ---------------- Unificação dos datasets Banlanço DESSEM ----------------

# Salvar na pasta de dataframe processados e unificados
# df_final = pd.concat(listaDatasets, ignore_index=True)

# df_final.to_parquet(f'datasets - parquet\DB\df_BalançoDessem_Unificado', index=False)
# print('Dataframe unificado e salvo na pasta DB')

