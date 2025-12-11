import re
import pandas as pd

def higienizar_telefone(tel):
    """
    Padroniza telefones para o formato 55 + DDD + número.
    Remove caracteres não numéricos e verifica validade básica.
    """
    if pd.isna(tel) or not str(tel).strip():
        return "INVÁLIDO"

    numeros = re.sub(r'\D', '', str(tel))
    
    # considera números nacionais (10 ou 11 dígitos)
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
