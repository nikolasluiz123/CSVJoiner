import pandas as pd
import re
from collections import defaultdict
import os

def agrupar_csv_por_nome(diretorio_entrada, diretorio_saida):
    os.makedirs(diretorio_saida, exist_ok=True)
    grupos = defaultdict(list)
    regex_data = re.compile(r'^\d{8}_')

    for arquivo in os.listdir(diretorio_entrada):
        if arquivo.endswith('.csv'):
            nome_limpo = regex_data.sub('', arquivo)
            caminho_completo = os.path.join(diretorio_entrada, arquivo)
            grupos[nome_limpo].append(caminho_completo)

    for nome, arquivos in grupos.items():
        dfs = [pd.read_csv(arq, encoding='ISO-8859-1', on_bad_lines='skip', sep=';') for arq in arquivos]
        df_combined = pd.concat(dfs, ignore_index=True)
        caminho_saida = os.path.join(diretorio_saida, f'{nome}')
        df_combined.to_csv(caminho_saida, index=False, encoding='ISO-8859-1', sep=';')
        print(f'Arquivo {caminho_saida} gerado com sucesso!')

diretorio_entrada = 'dados/dados_a3'
diretorio_saida = 'dados/dados_a3_tratados'

agrupar_csv_por_nome(diretorio_entrada, diretorio_saida)
