import pandas as pd
import os

# Caminho dos datasets 
pathData = 'datasets - parquet\DB\DB-NE'
os.makedirs(f'{pathData}', exist_ok=True)

# Listagens dos datasets
listaDatasets = os.listdir('datasets - parquet')

col_str = ['id_subsistema', 'nom_subsistema']
for data in listaDatasets:
    if data.startswith('datasets - Curva'):
        # Leitura do dataframe
        df = pd.read_parquet(f'datasets - parquet\{data}', engine='pyarrow')

        # Conversão para os tipos de dados corretos
        df['val_cargaenergiahomwmed'] = pd.to_numeric(df['val_cargaenergiahomwmed'])
        df[col_str] = df[col_str].convert_dtypes()

        # Filtrar só a região nordeste
        df_nordeste = df[df['id_subsistema'] == 'NE']

        # Renomar arquivo
        data_NE = data.replace('datasets','dfNE')

        # Salvar na pasta de dataframe processados
        df_nordeste.to_parquet(f'{pathData}\{data_NE}', index=False)
        print(f'{data_NE} salvo na pasta DB-NE')

