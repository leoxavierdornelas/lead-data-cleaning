import pandas as pd
import json
from src.cleaners import higienizar_telefone, validar_email

INPUT_CSV = 'data/leads_brutos.csv'
OUTPUT_QUAL = 'outputs/leads_qualificados.csv'
OUTPUT_INV = 'outputs/leads_invalidos.csv'
OUTPUT_METRICS = 'outputs/metrics.json'

def processar_leads(df_raw):
    """
    Processa DataFrame de leads.
    Retorna dois DataFrames: qualificados e inválidos.
    """
    df = df_raw.copy()
    # normaliza nomes das colunas
    df.columns = df.columns.str.strip()

    df['telefone_formatado'] = df['telefone'].apply(higienizar_telefone)
    df['email_valido'] = df['email'].apply(validar_email)
    df['lead_qualificado'] = df.apply(
        lambda x: 'SIM' if x['telefone_formatado'].startswith('55') and x['email_valido'] else 'NÃO',
        axis=1
    )

    df_qualificados = df[df['lead_qualificado'] == 'SIM'][['nome', 'telefone_formatado', 'lead_qualificado']]
    df_invalidos = df[df['lead_qualificado'] == 'NÃO'][['nome', 'telefone_formatado', 'lead_qualificado']]

    return df_qualificados, df_invalidos

def gerar_metricas(df_raw, df_qualificados, df_invalidos):
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

if __name__ == "__main__":
    df_raw = pd.read_csv(INPUT_CSV)
    df_qual, df_inv = processar_leads(df_raw)
    df_qual.to_csv(OUTPUT_QUAL, index=False)
    df_inv.to_csv(OUTPUT_INV, index=False)

    metrics = gerar_metricas(df_raw, df_qual, df_inv)
    with open(OUTPUT_METRICS, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, ensure_ascii=False, indent=4)

    print("Pipeline executado com sucesso!")
    print(metrics)
