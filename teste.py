import pandas as pd
import numpy as np

empresa0 = "data/silver/empresas0.parquet"

# Lemos o arquivo normalmente
df = pd.read_parquet(empresa0)

print(f"Total de linhas da tabela: {len(df)}")
print("-" * 30)

for coluna in df.columns:
    # 1. Contagem de NaNs (o que o Pandas acha que é nulo)
    nans = df[coluna].isna().sum()
    
    # 2. Contagem de texto real "Não informado"
    # Usamos .astype(str) aqui apenas para garantir a comparação textual
    txt_nao_informado = (df[coluna].astype(str) == "Não informado").sum()
    
    print(f"Coluna {coluna}:")
    print(f"  - NaNs detectados: {nans}")
    print(f"  - Textos 'Não informado': {txt_nao_informado}")

print("-" * 30)

# 3. VERIFICAÇÃO REAL (O "Tira-Teima")
# Vamos pegar uma linha que o Pandas diz que é NaN e ver o que tem escrito nela
coluna_teste = 'porte' 

if coluna_teste in df.columns:
    print(f"\nAnálise da coluna '{coluna_teste}':")
    # Agora buscamos onde está o "Não informado" em vez de buscar NaN
    total_nao_informado = (df[coluna_teste] == "Não informado").sum()
    print(f"Total de 'Não informado': {total_nao_informado}")
    
    # Mostra um exemplo real
    amostra = df[df[coluna_teste] == "Não informado"].iloc[0]
    print(f"Exemplo de linha com tratamento: \n{amostra}")
else:
    print(f"A coluna {coluna_teste} não existe neste arquivo. Colunas disponíveis: {df.columns.tolist()}")