import pandas as pd
import os

lista = os.path('datasets - parquet\DB')
print(lista)
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

lista = os.listdir('datasets - parquet\DB')

unidata = []
for data in lista:
    if data.startswith('dfNE - Balanco'):
        df = pd.read_parquet(f'datasets - parquet\DB\{data}')
        unidata.append(df)

dfFinal = pd.concat(unidata, ignore_index=True)

dfFinal.to_parquet(f'datasets - parquet\DB\dfNE - Balanco_Dessem_Unificado.parquet', engine='pyarrow', index=True)
print('Arquivo Salvo')

print(dfFinal)

