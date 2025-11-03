import requests
import os
import pandas as pd
from io import BytesIO

def baixar_datasets(API_url):
    # Buscar lista de recursos (arquivos)
    resp = requests.get(API_url)
    data = resp.json()
    resources = data["result"]["resources"]

    # Filtrar apenas arquivos Parquet
    csv_resources = [r for r in resources if r["url"].lower().endswith(".csv")]
    print(f"Encontrados {len(csv_resources)} arquivos .csv")

    # Baixar e ler todos
    for i, res in enumerate(csv_resources, 1):
        url = res["url"]
        name = res["name"]
        print(f"({i}/{len(csv_resources)}) Baixando {name} ...")

        r = requests.get(url)
        r.raise_for_status()

        # Lê diretamente em memória
        df = pd.read_csv(BytesIO(r.content), sep=";")
        # Salvar o consolidado
        output_file = os.path.join('datasets', f'datasets - {name}.csv')
        df.to_csv(output_file, index= False)

    return df

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

df_dadosHidro = baixar_datasets(url_dados_hidro)
# df_cargaEnergia = baixar_datasets(url_carga_energia)
# df_curvaEnergia = baixar_datasets(url_curva_energia)
# df_dessemDetalhado = baixar_datasets(url_dessem_detalhe)
