import requests
import os
import pandas as pd
from io import BytesIO

def unificador_datasets(API_url):
    dfs = []
    # Buscar lista de arquivos
    resp = requests.get(API_url)
    data = resp.json()
    resources = data["result"]["resources"]

    # Filtrar apenas arquivos Parquet
    parquet_resources = [r for r in resources if r["url"].lower().endswith(".parquet")]
    print(f"Encontrados {len(parquet_resources)} arquivos .parquet")

    # Baixar e ler todos
    for i, res in enumerate(parquet_resources, 1):
        url = res["url"]
        name = res["name"]
        print(f"({i}/{len(parquet_resources)}) Baixando {name} ...")

        r = requests.get(url)
        r.raise_for_status()

        # Lê diretamente em memória
        df = pd.read_parquet(BytesIO(r.content))
        dfs.append(df)

    # Juntar todos em um único DataFrame
    df_final = pd.concat(dfs, ignore_index=True)

    # Salvar o consolidado
    output_file = os.path.join('datasets unifcados',f'datasets {name}.parquet')
    df_final.to_parquet(output_file, compression="snappy")

    return df_final

ID_ONS = ['carga-energia',
            'curva-carga',
            'dados-hidrologicos-res',
            'carga-energia-verificada',
            'balanco_dessem_detalhe'
            ]

url_dados_hidro = "https://dados.ons.org.br/api/3/action/package_show?id=dados-hidrologicos-res"
url_carga_energia = "https://dados.ons.org.br/api/3/action/package_show?id=carga-energia"
url_curva_energia = "https://dados.ons.org.br/api/3/action/package_show?id=curva-carga"
url_dessem_detalhe = "https://dados.ons.org.br/api/3/action/package_show?id=balanco_dessem_detalhe"

df_dadosHidro = unificador_datasets(url_dados_hidro)
# df_cargaEnergia = unificador_datasets(url_carga_energia)
# df_curvaEnergia = unificador_datasets(url_curva_energia)
# df_dessemDetalhado = unificador_datasets(url_dessem_detalhe)

