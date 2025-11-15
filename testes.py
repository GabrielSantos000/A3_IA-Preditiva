import pandas as pd

df = pd.read_parquet('datasets - parquet\datasets - Dados_Hidrologicos_Res-2018.parquet')

# Colunas para excluir
col_exc = ['nom_bacia',
'nom_ree',
'nom_reservatorio',
'num_ordemcs',
'cod_usina',
'val_niveljusante',
'val_vazaoartificial',
'val_vazaooutrasestruturas',
'val_vazaotransferida',
'id_reservatorio',
'val_vazaousoconsuntivo',
'val_vazaoevaporacaoliquida'] 

# Colunas para converter em valores em tipo real (float)
col_float = ['val_nivelmontante',
'val_volumeutilcon',
'val_vazaoafluente',
'val_vazaoturbinada',
'val_vazaovertida',
'val_vazaodefluente',
'val_vazaonatural',
'val_vazaoincremental']

# Colunas para converte em tipo texto (string)
col_str = ['id_subsistema', 'nom_subsistema','tip_reservatorio']

 # Conversão para os tipos de dados corretos
df[col_float] = df[col_float].apply(pd.to_numeric, errors='coerce') # Para real
df[col_str] = df[col_str].convert_dtypes()

# Exclusão das colunas desnecessárias para a previsão
df = df.drop(columns = col_exc, axis=1)

# Calcula a mediana da coluna de vazão incremental e substituir os NaN por ela.
mediana = df["val_vazaoincremental"].median()
df["val_vazaoincremental"].fillna(mediana, inplace=True)

# Filtrar só a região nordeste e do tipo de reservatório com usina
df_ne = df[df['id_subsistema'] == 'NE']  
df_res = df_ne[df_ne['tip_reservatorio'] == 'Reservatório com Usina']


print(df_res['val_vazaoincremental'].isna())
print(df['val_vazaoincremental'].isna())