import re


def normalize_numeric_value(value_str):
    """
    Normaliza uma string que representa um valor numérico.
    VERSÃO CORRIGIDA - processa Bilhão/Milhão corretamente
    """
    if value_str is None or not isinstance(value_str, str) or value_str.strip() in ['-', '', '--']:
        return None

    cleaned_str = value_str.lower().strip()

    # Remove prefixos e sufixos não numéricos
    cleaned_str = cleaned_str.replace('r$', '').replace('%', '').strip()

    # Identifica multiplicadores (bilhões, milhões)
    multiplier = 1
    
    # CORREÇÃO: Remover palavras completas primeiro, antes de processar
    if 'bilh' in cleaned_str:
        multiplier = 1_000_000_000
        # Remove todas as variações de bilhão/bilhões
        cleaned_str = (cleaned_str.replace('bilhões', '')
                                  .replace('bilhoes', '')
                                  .replace('bilhão', '')
                                  .replace('bilhao', '')
                                  .strip())
    elif 'milh' in cleaned_str:
        multiplier = 1_000_000
        # Remove todas as variações de milhão/milhões
        cleaned_str = (cleaned_str.replace('milhões', '')
                                  .replace('milhoes', '')
                                  .replace('milhão', '')
                                  .replace('milhao', '')
                                  .strip())

    # Padroniza separadores (resto do código igual)
    if ',' in cleaned_str and '.' in cleaned_str:
        pos_virgula = cleaned_str.rfind(',')
        pos_ponto = cleaned_str.rfind('.')
        
        if pos_virgula > pos_ponto:
            cleaned_str = cleaned_str.replace('.', '').replace(',', '.')
        else:
            cleaned_str = cleaned_str.replace(',', '')
    elif ',' in cleaned_str:
        if cleaned_str.count(',') > 1:
            cleaned_str = cleaned_str.replace(',', '')
        else:
            partes = cleaned_str.split(',')
            if len(partes) == 2 and len(partes[1]) <= 3:
                cleaned_str = cleaned_str.replace(',', '.')
            else:
                cleaned_str = cleaned_str.replace(',', '')
    elif '.' in cleaned_str:
        if cleaned_str.count('.') > 1:
            cleaned_str = cleaned_str.replace('.', '')
        else:
            partes = cleaned_str.split('.')
            if len(partes) == 2:
                if len(partes[1]) <= 2:
                    pass
                elif len(partes[0]) <= 3:
                    if len(partes[1]) == 3:
                        cleaned_str = cleaned_str.replace('.', '')
                else:
                    if len(partes[1]) == 3:
                        cleaned_str = cleaned_str.replace('.', '')

    try:
        numeric_value = float(cleaned_str)
        final_value = numeric_value * multiplier

        if final_value.is_integer():
            return int(final_value)
        return final_value
    except (ValueError, TypeError):
        return None
