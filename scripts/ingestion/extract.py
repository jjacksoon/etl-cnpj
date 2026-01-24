import requests
import os
import zipfile
from datetime import datetime

def download_cnpj_data(nome_arquivo):
    # --- 1. DEFINIÇÃO DA DATA ---
    hoje = datetime.now()
    ano_mes = hoje.strftime('%Y-%m')  # verificando ano-mes do arquivo pra saber se tem que baixar novo ou não'

    # --- 2. CONFIGURAÇÃO DE URLS E PASTAS ---
    url_base = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{ano_mes}/"
    url_completa = f"{url_base}/{nome_arquivo}"
    
    # Descobre onde o script está agora e sobe duas pastas para achar a 'data'
    script_dir = os.path.dirname(os.path.abspath(__file__)) # pasta 'ingestion'
    raiz_projeto = os.path.dirname(os.path.dirname(script_dir)) # volta para 'etl-cnpj'
    
    pasta_bronze = os.path.join(raiz_projeto, "data", "bronze")
    caminho_final = os.path.join(pasta_bronze, nome_arquivo)

    # ---- VERIFICAÇÃO DE EXISTENCIA DE ARQUIVO ----
    if os.path.exists(caminho_final):
        if zipfile.is_zipfile(caminho_final):
            print(f"O arquivo {nome_arquivo} já existe.")
            return

    # --- 3. CRIAÇÃO DA PASTA ---
    if not os.path.exists(pasta_bronze):
        os.makedirs(pasta_bronze)
        print(f"Pasta criada em: {pasta_bronze}")

    # --- 4. DOWNLOAD ---
    print(f"Tentando baixar de: {url_completa}")
    
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        # Iniciando a conexão
        resposta = requests.get(url_completa, stream=True, headers=headers, timeout=300)
        
        # Se o arquivo existir (Status 200)
        if resposta.status_code == 200:
            print(f"Sucesso! Baixando {nome_arquivo}...")
            
            with open(caminho_final, "wb") as f:
                for chunk in resposta.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # --- 5. VALIDAÇÃO DO ZIP ---
            if zipfile.is_zipfile(caminho_final):
                print(f"CONCLUÍDO: O arquivo está em {caminho_final}")
            else:
                print("AVISO: O arquivo baixado não parece ser um ZIP válido.")
        
        else:
            print(f"ERRO: O site retornou status {resposta.status_code}.")

    except Exception as e:
        print(f"Erro na extração: {e}")
        raise e

if __name__ == "__main__":
    # Testando apenas com o arquivo de CNAEs por ser leve
    arquivos_download =[
        "Socios0.zip",
        "Qualificacoes.zip",
        "Paises.zip",
        "Naturezas.zip",
        "Municipios.zip",
        "Estabelecimentos0.zip",
        "Empresas0.zip",
        "Cnaes.zip"
    ]

    print(f"----Iniciando a extração de {len(arquivos_download)} arquivos ----")

    for arquivo in arquivos_download:
        try:
            download_cnpj_data(arquivo)
        except Exception as e:
            print(f"Falha ao processar {arquivo}. Pulando ao próximo...")
            continue
    
    print("Processo de extração finalizado!")