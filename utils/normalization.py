import re

def normalize_numeric_value(value_str):
    """
    Normaliza uma string que representa um valor numérico para um int ou float puro.
    Lida com R$, %, pontos, vírgulas, milhões (M/Mi) e bilhões (B/Bi).

    Exemplos:
    - "12,93" -> 12.93
    - "7,96%" -> 7.96
    - "R$ 207,42 Bilhões" -> 207420000000
    - "207.738.000.000" -> 207738000000
    - "-34,13" -> -34.13
    - "-" ou "" ou None -> None
    """
    if value_str is None or not isinstance(value_str, str) or value_str.strip() in ['-', '']:
        return None

    cleaned_str = value_str.lower().strip()
    
    # Remove prefixos e sufixos não numéricos
    cleaned_str = cleaned_str.replace('r$', '').replace('%', '').strip()

    # Identifica multiplicadores (bilhões, milhões)
    multiplier = 1
    if 'bi' in cleaned_str:
        multiplier = 1_000_000_000
        cleaned_str = re.sub(r'bi(lhões)?', '', cleaned_str).strip()
    elif 'mi' in cleaned_str:
        multiplier = 1_000_000
        cleaned_str = re.sub(r'mi(lhões)?', '', cleaned_str).strip()

    # Padroniza separadores: remove pontos de milhar, troca vírgula decimal por ponto
    # Esta ordem é crucial. "1.234,56" -> "1234,56" -> "1234.56"
    cleaned_str = cleaned_str.replace('.', '').replace(',', '.')

    try:
        numeric_value = float(cleaned_str)
        final_value = numeric_value * multiplier

        # Retorna como int se for um número inteiro, senão como float
        if final_value.is_integer():
            return int(final_value)
        return final_value
    except (ValueError, TypeError):
        # Retorna None se, mesmo após a limpeza, não for um número válido
        return None

# Bloco para testar a função de forma independente
if __name__ == '__main__':
    test_cases = [
        "12,93", "2,25", "7,96%", "103,34%",
        "R$ 207,42 Bilhões", "15,76 Bilhões",
        "363.321.000", "-16.946.400.000",
        "13.16", "5.68", "-2.05", "186922.149.83",
        "1.11", "-", None, "N/A"
    ]
    for case in test_cases:
        print(f'Original: "{case}" -> Normalizado: {normalize_numeric_value(case)}')