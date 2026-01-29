import os
import zipfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def transform_to_silver(nome_arquivo, colunas):
    # 1. MAPEAMENTO DE CAMINHOS
    script_dir = os.path.dirname(os.path.abspath(__file__))
    raiz_projeto = os.path.dirname(os.path.dirname(script_dir))
    pasta_bronze = os.path.join(raiz_projeto, "data", "bronze")
    pasta_silver = os.path.join(raiz_projeto, "data", "silver")
    caminho_zip = os.path.join(pasta_bronze, nome_arquivo)

    if not os.path.exists(pasta_silver):
        os.makedirs(pasta_silver)

    print(f"\n🚀 Processando (Modo Writer): {nome_arquivo}")

    try:
        with zipfile.ZipFile(caminho_zip, 'r') as z:
            arquivo_interno = z.namelist()[0]

            with z.open(arquivo_interno) as f:
                # keep_default_na=False impede a criação de objetos NaN
                df_iter = pd.read_csv(
                    f, sep=';', encoding='latin-1', header=None,
                    names=colunas, dtype=str, chunksize=100000,
                    keep_default_na=False, na_values=[]
                )

                nome_saida = nome_arquivo.replace(".zip", ".parquet").lower()
                caminho_saida = os.path.join(pasta_silver, nome_saida)
                
                # Deleta arquivo antigo se existir para não dar conflito de estrutura
                if os.path.exists(caminho_saida):
                    os.remove(caminho_saida)

                # Inicializa o 'writer' como None para criá-lo apenas no primeiro chunk
                writer = None 

                # 2. LOOP DE PROCESSAMENTO POR CHUNKS
                for i, chunk in enumerate(df_iter):
                    # TRATAMENTO: Limpeza de espaços e preenchimento de vazios
                    for col in chunk.columns:
                        chunk[col] = chunk[col].str.strip()
                        mask = (chunk[col] == "") | (chunk[col].str.lower() == "nan")
                        chunk.loc[mask, col] = "Não informado"
                    
                    # 3. CONVERSÃO E ESCRITA VIA PYARROW (O QUE EVITA O ERRO)
                    # Converte o DataFrame do Pandas em uma Tabela do PyArrow
                    table = pa.Table.from_pandas(chunk)

                    # No primeiro chunk, criamos o arquivo e definimos o esquema (colunas)
                    if writer is None:
                        writer = pq.ParquetWriter(caminho_saida, table.schema)
                    
                    # Grava o pedaço atual no arquivo aberto
                    writer.write_table(table)
                    
                    #Dando visibilidade de quantas linhas do arquivo estão sendo processadas por vez
                    if i % 10 == 0:
                        print(f"   📊 {(i+1)*100000} linhas persistidas...")

                # 4. FINALIZAÇÃO
                # Muito importante: Fecha o gravador para salvar os metadados do Parquet
                if writer:
                    writer.close()

                print(f"✅ Finalizado com sucesso: {caminho_saida}")

    except Exception as e:
        print(f"❌ Erro ao processar {nome_arquivo}: {e}")

# DICIONÁRIO DE COLUNAS (Permanece igual)
dicionario_colunas = {
    "Cnaes.zip": ["codigo", "descricao"],
    "Empresas0.zip": ["cnpj", "razao social", "natureza juridica", "qualificacao responsavel", "capital social", "porte"],
    "Estabelecimentos0.zip": ["cnpj", "cpj ordem", "cnpj dv", "id matriz", "nome fantasia", "situacao cadastral", "data situacao", "motivo situacao", "cidade exterior", "pais", "inicio atividade", "cnae principal", "cnae secundario", "tipo logradouro", "logradouro", "numero", "complemento", "bairro", "cep", "uf", "municipio", "ddd1", "telefone1", "ddd2", "telefone2", "ddd fax", "fax", "correio eletronico", "situacao", "data situacao_extra"],
    "Municipios.zip": ["codigo", "descricao"],
    "Naturezas.zip": ["codigo", "descricao"],
    "Paises.zip": ["codigo", "descricao"],
    "Qualificacoes.zip": ["codigo", "descricao"],
    "Socios0.zip": ["cnpj", "id socio", "nome socio", "cnpj socio", "qualificacao", "data entrada", "pais", "representante", "nome representante", "qualificacao representante", "faixa etaria"]
}

if __name__ == "__main__":
    print("--- 🛠️ Iniciando Transformação Silver (Modo Stream) ---")
    for arquivo, colunas in dicionario_colunas.items():
        transform_to_silver(arquivo, colunas)
    print("\n✨ Processo concluído com sucesso!")