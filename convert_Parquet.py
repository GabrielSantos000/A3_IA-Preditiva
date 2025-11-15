import pandas as pd
import os

# Conversão de .csv para .parquet
def toParquet(dataset_csv):
    #Lê o arquivo e decodifica
    try:
        df = pd.read_csv(f'{dataset_csv}', encoding='utf-8', sep=',')
    except UnicodeDecodeError:
        df = df = pd.read_csv(f'{dataset_csv}', encoding='latin1', sep=',')

    #Renomar o arquivo
    dfParquet = os.path.splitext(f'{dataset_csv}')[0]+'.parquet'
    
    # Converter o arquivo
    df.to_parquet(dfParquet, engine='pyarrow', index=True)
    return dfParquet, 'Arquivo salvo com sucesso'

data = 'datasets - parquet\dados_filtrados_nordeste_2000_2025.csv'
toParquet(data)