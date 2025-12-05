import pandas as pd
import re

# --- CENÁRIO ---
# Problema clássico de CRM: Clientes enviam listas de contatos com formatações 
# variadas (com parenteses, sem DDD, com traços), o que falha nos disparadores de SMS/Voz.

raw_leads = [
    {'nome': 'Loja A', 'telefone': '(19) 98282-6121', 'email': 'loja_a@gmail.com', 'cnpj': '40.288.544/0001-15'}, # Formato padrão
    {'nome': 'Cliente B', 'telefone': '11999998888', 'email': 'cliente.b@hotmail', 'cnpj': '33222111000199'}, # Sem formatação
    {'nome': 'Lead C', 'telefone': '19 3232-4040', 'email': 'contato@empresa.com', 'cnpj': ''}, # Fixo
    {'nome': 'Erro D', 'telefone': '999-999', 'email': 'erro@teste', 'cnpj': 'nan'}, # Incompleto
]

print("--- BASE DE LEADS BRUTA (SUJA) ---")
df_raw = pd.DataFrame(raw_leads)
print(df_raw[['nome', 'telefone']])

# --- FUNÇÕES DE LIMPEZA (DATA QUALITY) ---

def higienizar_telefone(tel):
    """
    Padroniza telefones para o formato de disparo (55 + DDD + Numero).
    Remove caracteres não numéricos.
    """
    if pd.isna(tel): return "INVÁLIDO"
    
    # Remove tudo que não é dígito
    apenas_numeros = re.sub(r'\D', '', str(tel))
    
    # Lógica de validação básica Brasil
    if len(apenas_numeros) < 10 or len(apenas_numeros) > 11:
        return "VERIFICAR MANUALMENTE"
    
    # Adiciona código do país se não tiver
    return f"55{apenas_numeros}"

def validar_email(email):
    """Verifica se o email possui estrutura válida (@ e dominio)"""
    if pd.isna(email) or '@' not in str(email) or '.' not in str(email):
        return False
    return True

# --- PIPELINE DE EXECUÇÃO ---

print("\n--- INICIANDO HIGIENIZAÇÃO DE CONTATOS ---")

df_clean = df_raw.copy()

# 1. Padronização de Telefones (Crucial para SMS/Voz)
df_clean['telefone_formatado'] = df_clean['telefone'].apply(higienizar_telefone)

# 2. Validação de Emails
df_clean['email_valido'] = df_clean['email'].apply(validar_email)

# 3. Flag de Leads Qualificados (Têm telefone OK e Email OK)
df_clean['lead_qualificado'] = df_clean.apply(
    lambda x: 'SIM' if x['email_valido'] and '55' in x['telefone_formatado'] else 'NÃO', 
    axis=1
)

# Filtra apenas o necessário para upload no CRM
df_final = df_clean[['nome', 'telefone_formatado', 'lead_qualificado']]

print("\n--- LEADS PRONTOS PARA IMPORTAÇÃO ---")
print(df_final)

# Exporta lista limpa para o time de vendas
df_final.to_csv('lista_disparo_limpa.csv', index=False)