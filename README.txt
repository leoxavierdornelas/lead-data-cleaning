# ==============================
# README.txt
# ==============================
Lead Data Hygiene & ETL Pipeline

Descrição:
Pipeline de Engenharia de Dados focado em Qualidade de Dados (Data Quality) para CRM. O script ingere listas de contatos despadronizadas e realiza a higienização automática dos números de telefone (adicionando DDI 55, removendo máscaras) e validação de e-mails.

Funcionalidades:
- Regex robusto para limpeza de caracteres especiais.
- Padronização de formato: 55 + DDD + número.
- Flag automática de leads qualificados vs. inválidos.
- Separação de leads inválidos em arquivo separado.
- Geração de métricas resumidas em JSON.
- Estrutura modular para fácil manutenção e escalabilidade.

Tecnologias:
- Python 3.11+
- Pandas
- Regular Expressions (Regex)
- JSON/CSV

Estrutura do repositório:
- src/
    - etl_pipeline.py
    - utils.py
- run_pipeline.py
- data/ (entrada)
- outputs/ (saída)
- README.txt
- requirements.txt

Exemplo de uso:
1. Coloque suas listas de leads em CSV dentro da pasta data/
2. Execute: python run_pipeline.py
3. Saídas:
   - outputs/leads_qualificados.csv
   - outputs/leads_invalidos.csv
   - outputs/metrics.json

# ==============================
# src/utils.py
# ==============================
import re
import pandas as pd

def higienizar_telefone(tel):
    """
    Padroniza telefones para o formato de disparo (55 + DDD + Número).
    Remove caracteres não numéricos e verifica validade básica.
    """
    if pd.isna(tel) or not str(tel).strip():
        return "INVÁLIDO"

    numeros = re.sub(r'\D', '', str(tel))
    
    if len(numeros) == 10 or len(numeros) == 11:
        return f"55{numeros}"
    return "INVÁLIDO"

def validar_email(email):
    """
    Valida e-mail com regex simples. Retorna True se válido.
    """
    if pd.isna(email):
        return False
    regex = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return re.match(regex, email) is not None

# ==============================
# src/etl_pipeline.py
# ==============================
import pandas as pd
from src.utils import higienizar_telefone, validar_email

def processar_leads(df_raw):
    """
    Processa DataFrame de leads e retorna:
    - df_qualificados: leads com telefone e email válidos
    - df_invalidos: leads com telefone ou email inválidos
    """
    df = df_raw.copy()
    df['telefone_formatado'] = df['telefone'].apply(higienizar_telefone)
    df['email_valido'] = df['email'].apply(validar_email)
    df['lead_qualificado'] = df.apply(
        lambda x: 'SIM' if x['email_valido'] and x['telefone_formatado'].startswith('55') else 'NÃO',
        axis=1
    )
    
    df_qualificados = df[df['lead_qualificado'] == 'SIM'][['nome', 'telefone_formatado', 'lead_qualificado']]
    df_invalidos = df[df['lead_qualificado'] == 'NÃO'][['nome', 'telefone_formatado', 'lead_qualificado']]
    
    return df_qualificados, df_invalidos

def gerar_metricas(df_raw, df_qualificados, df_invalidos):
    """
    Gera métricas básicas sobre o processamento de leads.
    """
    total = len(df_raw)
    qtd_qualificados = len(df_qualificados)
    qtd_invalidos = len(df_invalidos)
    
    return {
        'total_leads': total,
        'qualificados': qtd_qualificados,
        'invalidos': qtd_invalidos,
        'percentual_qualificados': round((qtd_qualificados / total) * 100, 2),
        'percentual_invalidos': round((qtd_invalidos / total) * 100, 2)
    }

# ==============================
# run_pipeline.py
# ==============================
import pandas as pd
import json
from src.etl_pipeline import processar_leads, gerar_metricas

# Caminhos
INPUT_CSV = 'data/leads_brutos.csv'
OUTPUT_QUAL = 'outputs/leads_qualificados.csv'
OUTPUT_INV = 'outputs/leads_invalidos.csv'
OUTPUT_METRICS = 'outputs/metrics.json'

# --- EXECUÇÃO ---
df_raw = pd.read_csv(INPUT_CSV)

df_qualificados, df_invalidos = processar_leads(df_raw)

# Exporta resultados
df_qualificados.to_csv(OUTPUT_QUAL, index=False)
df_invalidos.to_csv(OUTPUT_INV, index=False)

metrics = gerar_metricas(df_raw, df_qualificados, df_invalidos)
with open(OUTPUT_METRICS, 'w', encoding='utf-8') as f:
    json.dump(metrics, f, ensure_ascii=False, indent=4)

print("Pipeline executado com sucesso!")
print(f"Leads qualificados: {len(df_qualificados)}")
print(f"Leads inválidos: {len(df_invalidos)}")
print(f"Métricas geradas em {OUTPUT_METRICS}")