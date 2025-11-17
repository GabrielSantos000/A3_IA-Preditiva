import pandas as pd
import os

path = 'datasets - parquet\datasets originais\DB-processando'

# Conversão de .csv para .parquet
def toParquet(dataset_csv):
    #Lê o arquivo e decodifica
    try:
        df = pd.read_csv(f'{dataset_csv}', encoding='utf-8', sep=';')
    except UnicodeDecodeError:
        df = df = pd.read_csv(f'{dataset_csv}', encoding='latin1', sep='.')

    #Renomar o arquivo
    dfParquet = os.path.basename(f'{dataset_csv}').replace('.csv','.parquet')
    
    # Converter o arquivo
    df.to_parquet(f'{path}\{dfParquet}', engine='pyarrow', index=True)
    return dfParquet, 'Arquivo salvo com sucesso'

# Conversão de .parquet para .csv
def toCsv(dataset_parquet):
    #Lê o arquivo e decodifica
    df = pd.read_parquet(f'{dataset_parquet}', engine='pyarrow')

    #Renomar o arquivo
    dfcsv = os.path.basename(data)+'.csv'
    
    # Converter o arquivo
    df.to_csv(f'{path}\{dfcsv}', sep=';', encoding='latin1',index=True)
    return dfcsv, 'Arquivo salvo com sucesso'

toParquet('testes\CONSUMO_E_NUMCONS_SAM_UF.parquet.csv')
toParquet('testes\CONSUMO_E_NUMCONS_SAM.parquet.csv')