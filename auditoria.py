"""
Auditoria de pagamentos - equipe externa S&A Imunizações
Regras de validação, deduplicação e transformação para o formato do financeiro.
"""
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
 
 
# ============================================================
# MAPEAMENTO FIXO: Tipo de Despesa -> Categoria Financeira
# (extraído da aba "Categorias" da Planilha - Financeiro)
# ============================================================
CATEGORIA_MAP = {
    'Ajuda de custo para Motorista': 'Diárias - Equipe externa',
    'Aplicativo de Transporte': 'DV - Pedágio/Passagem/Uber',
    'Carro Próprio': 'DV - Locação de veículos',
    'Combustível': 'DV - Combustível',
    'Deslocamento KM': 'DV - Locação de veículos',
    'Estacionamento': 'DV - Pedágio/Passagem/Uber',
    'Pedágio': 'DV - Pedágio/Passagem/Uber',
    'Pernoite': 'DV - Hospedagem',
    'Ajuda de custo': 'Diárias - Equipe externa',
    'Adicional de diária': 'Diárias - Equipe externa',
    'Diária T + VT + VA': 'Diárias - Equipe externa',
    'Diária A + VT + VA': 'Diárias enfermeiros e adm',
    'Diária M + VT + VA': 'Diárias motoristas',
    'Alimentação': 'DV - Alimentação',
    'Retirada de Doses': 'Diárias - Equipe externa',
}
 
 
# ============================================================
# UTILITÁRIOS DE FORMATAÇÃO
# ============================================================
 
def parse_valor(s):
    """Converte 'R$1.234,56' -> 1234.56"""
    if pd.isna(s):
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s).replace('R$', '').replace(' ', '').strip()
    s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0
 
 
def formatar_cpf(cpf):
    """Recebe CPF (string ou numérico) e devolve no formato 000.000.000-00."""
    if pd.isna(cpf) or cpf is None:
        return ''
    cpf_str = ''.join(filter(str.isdigit, str(cpf)))
    if not cpf_str:
        return ''
    cpf_str = cpf_str.zfill(11)[:11]
    if len(cpf_str) != 11:
        return str(cpf)
    return f'{cpf_str[:3]}.{cpf_str[3:6]}.{cpf_str[6:9]}-{cpf_str[9:]}'
 
 
def cpf_digits(cpf):
    """Retorna apenas dígitos do CPF, sempre 11 chars."""
    if pd.isna(cpf) or cpf is None:
        return ''
    s = ''.join(filter(str.isdigit, str(cpf)))
    return s.zfill(11)[:11] if s else ''
 
 
def validar_cpf(cpf):
    """Valida CPF pelos dígitos verificadores."""
    cpf_str = cpf_digits(cpf)
    if len(cpf_str) != 11 or cpf_str == cpf_str[0] * 11:
        return False
    soma = sum(int(cpf_str[i]) * (10 - i) for i in range(9))
    d1 = (soma * 10 % 11) % 10
    if d1 != int(cpf_str[9]):
        return False
    soma = sum(int(cpf_str[i]) * (11 - i) for i in range(10))
    d2 = (soma * 10 % 11) % 10
    return d2 == int(cpf_str[10])
 
 
def parse_data(s):
    if pd.isna(s):
        return pd.NaT
    if isinstance(s, (datetime, pd.Timestamp)):
        return pd.Timestamp(s)
    try:
        return pd.to_datetime(s, dayfirst=True, errors='coerce')
    except Exception:
        return pd.NaT
 
 
def formatar_pix(valor, tipo):
    """
    Formata a chave PIX de acordo com o tipo:
    - Telefone/Celular: +55 + dígitos (ex.: +5517996720550)
    - CPF/CNPJ: 000.000.000-00
    - Email/Aleatória: mantém como está
    """
    if pd.isna(valor) or str(valor).strip() == '':
        return ''
    valor_str = str(valor).strip()
    tipo_norm = (str(tipo) if not pd.isna(tipo) else '').lower()
 
    if 'telefone' in tipo_norm or 'celular' in tipo_norm:
        digits = ''.join(filter(str.isdigit, valor_str))
        if not digits:
            return valor_str
        # Se já começa com 55 e tem 12-13 dígitos, mantém
        if digits.startswith('55') and len(digits) in (12, 13):
            return f'+{digits}'
        return f'+55{digits}'
    if 'cpf' in tipo_norm or 'cnpj' in tipo_norm:
        digits = ''.join(filter(str.isdigit, valor_str))
        if len(digits) == 11:
            return formatar_cpf(digits)
        return valor_str
    return valor_str  # email, aleatória
 
 
# ============================================================
# CARGA
# ============================================================
 
RELATORIO_COLS_OBRIG = ['Valor', 'CPF', 'Nome do profissional', 'Tipo de Despesa',
                        'Código da tarefa', 'Data da despesa', 'id_despesa']
 
CADASTRO_COLS_OBRIG = ['Chave PIX', 'Número do CPF', 'Tipo de chave PIX']
 
 
def _ler_csv_robusto(arquivo):
    """Lê CSV tentando encodings comuns (utf-8 com/sem BOM, latin-1)."""
    for enc in ('utf-8-sig', 'utf-8', 'latin-1', 'cp1252'):
        try:
            arquivo.seek(0) if hasattr(arquivo, 'seek') else None
            df = pd.read_csv(arquivo, encoding=enc, sep=None, engine='python')
            # Limpa BOM eventual no nome da primeira coluna
            df.columns = [c.lstrip('\ufeff').strip() for c in df.columns]
            return df
        except (UnicodeDecodeError, UnicodeError):
            continue
    raise ValueError('Não foi possível ler o CSV (encoding não suportado).')
 
 
def carregar_relatorio(arquivo):
    """Carrega CSV do Bubble e adiciona colunas auxiliares."""
    df = _ler_csv_robusto(arquivo)
 
    faltam = [c for c in RELATORIO_COLS_OBRIG if c not in df.columns]
    if faltam:
        raise ValueError(
            f'Arquivo do Relatório está com colunas faltando: {", ".join(faltam)}.\n\n'
            f'Colunas encontradas: {", ".join(df.columns[:8])}...\n\n'
            f'Verifique se você subiu o CSV correto no campo "Relatório (CSV)" '
            f'(o arquivo exportado do Bubble com despesas pendentes de pagamento).'
        )
 
    df['_valor_num'] = df['Valor'].apply(parse_valor)
    df['_cpf_digits'] = df['CPF'].apply(cpf_digits)
    df['_cpf_fmt'] = df['CPF'].apply(formatar_cpf)
    df['_data_despesa'] = df['Data da despesa'].apply(parse_data)
    df['_idx'] = df.index
    return df
 
 
def carregar_cadastro(arquivo):
    """
    Carrega cadastro _EXT_ Profissional. Aceita xlsx ou csv.
    Espera colunas: 'Chave PIX', 'Nome completo', 'Número do CPF', 'Tipo de chave PIX'.
    A coluna 'Fórmula (Correção de PIX)' é opcional - se não existir,
    a chave PIX é formatada na hora a partir de 'Chave PIX' + 'Tipo de chave PIX'.
    """
    nome = getattr(arquivo, 'name', str(arquivo)).lower()
    if nome.endswith('.csv'):
        df = _ler_csv_robusto(arquivo)
    else:
        try:
            df = pd.read_excel(arquivo, sheet_name='_EXT_ Profissional')
        except Exception:
            df = pd.read_excel(arquivo)
        df.columns = [str(c).lstrip('\ufeff').strip() for c in df.columns]
 
    faltam = [c for c in CADASTRO_COLS_OBRIG if c not in df.columns]
    if faltam:
        raise ValueError(
            f'Arquivo do Cadastro está com colunas faltando: {", ".join(faltam)}.\n\n'
            f'Colunas encontradas: {", ".join(df.columns[:8])}...\n\n'
            f'Verifique se você subiu o cadastro _EXT_ Profissional correto '
            f'no campo "Cadastro" (não o relatório de despesas).'
        )
 
    df['_cpf_digits'] = df['Número do CPF'].apply(cpf_digits)
    return df
 
 
def lookup_pix_cadastro(cpf_digits_value, cadastro_df):
    """
    Busca chave PIX formatada do cadastro pelo CPF.
    Se houver coluna 'Fórmula (Correção de PIX)' usa direto;
    caso contrário, formata na hora a partir de 'Chave PIX' + 'Tipo de chave PIX'.
    """
    if not cpf_digits_value or cadastro_df is None or cadastro_df.empty:
        return None, None
    match = cadastro_df[cadastro_df['_cpf_digits'] == cpf_digits_value]
    if match.empty:
        return None, None
    row = match.iloc[0]
    tipo = row.get('Tipo de chave PIX')
    tipo = str(tipo).strip() if not pd.isna(tipo) else None
 
    pix_formula = row.get('Fórmula (Correção de PIX)') if 'Fórmula (Correção de PIX)' in cadastro_df.columns else None
    pix_bruto = row.get('Chave PIX')
 
    if pix_formula is not None and not pd.isna(pix_formula) and str(pix_formula).strip():
        return str(pix_formula).strip(), tipo
    if pix_bruto is not None and not pd.isna(pix_bruto) and str(pix_bruto).strip():
        return formatar_pix(pix_bruto, tipo), tipo
    return None, None
 
 
# ============================================================
# AUDITORIA - REGRAS
# ============================================================
 
def detectar_duplicatas_exatas(df):
    """
    Duplicatas EXATAS: mesmo id_despesa.
    Retorna índices a remover (mantém a primeira ocorrência).
    """
    if 'id_despesa' not in df.columns:
        return []
    duplicadas = df[df.duplicated(subset=['id_despesa'], keep='first')]
    return duplicadas.index.tolist()
 
 
def detectar_duplicatas_suspeitas(df):
    """
    Suspeitas: mesmo CPF + Código da tarefa + Tipo de Despesa + Valor.
    NÃO remove automaticamente - retorna grupos para revisão.
    """
    chave = ['_cpf_digits', 'Código da tarefa', 'Tipo de Despesa', '_valor_num']
    suspeitas = df[df.duplicated(subset=chave, keep=False)].copy()
    if suspeitas.empty:
        return suspeitas, []
    suspeitas = suspeitas.sort_values(chave + ['_data_despesa'])
    suspeitas['_grupo_dup'] = suspeitas.groupby(chave).ngroup() + 1
    grupos = sorted(suspeitas['_grupo_dup'].unique().tolist())
    return suspeitas, grupos
 
 
def detectar_sobreposicao_tarefas(df):
    """
    Sobreposição: mesmo profissional + mesma data + mesmo TIPO de despesa
    em tarefas DIFERENTES.
 
    Exemplo: duas Diárias T+VT+VA em tarefas distintas no mesmo dia → sobreposição.
    Já uma Diária + um Pernoite + um Pedágio no mesmo dia em tarefas diferentes
    é NORMAL (despesas distintas que ocorrem juntas).
    """
    base = df.dropna(subset=['_cpf_digits', 'Código da tarefa', '_data_despesa', 'Tipo de Despesa']).copy()
    base = base[base['_cpf_digits'] != '']
    if base.empty:
        return pd.DataFrame()
 
    cont = (
        base.groupby(['_cpf_digits', '_data_despesa', 'Tipo de Despesa'])['Código da tarefa']
        .nunique()
        .reset_index(name='qtd_tarefas')
    )
    suspeitos = cont[cont['qtd_tarefas'] > 1]
    if suspeitos.empty:
        return pd.DataFrame()
 
    chaves = set(zip(
        suspeitos['_cpf_digits'],
        suspeitos['_data_despesa'],
        suspeitos['Tipo de Despesa'],
    ))
    mask = base.apply(
        lambda r: (r['_cpf_digits'], r['_data_despesa'], r['Tipo de Despesa']) in chaves,
        axis=1,
    )
    return base[mask].sort_values(['_cpf_digits', '_data_despesa', 'Tipo de Despesa', 'Código da tarefa'])
 
 
def detectar_cpfs_invalidos(df):
    df = df.copy()
    df['_cpf_valido'] = df['_cpf_digits'].apply(validar_cpf)
    invalidos = df[~df['_cpf_valido'] & (df['_cpf_digits'] != '')]
    sem_cpf = df[df['_cpf_digits'] == '']
    return invalidos, sem_cpf
 
 
def detectar_pix_faltante(df, cadastro_df):
    """Linhas sem PIX no relatório - tenta achar no cadastro."""
    sem_pix = df[df['Chave PIX'].isna() | (df['Chave PIX'].astype(str).str.strip() == '')].copy()
    if sem_pix.empty:
        sem_pix['_pix_cadastro'] = None
        sem_pix['_pix_tipo_cadastro'] = None
        sem_pix['_pix_resolvido'] = False
        return sem_pix
 
    pix_lookup = sem_pix['_cpf_digits'].apply(lambda c: lookup_pix_cadastro(c, cadastro_df))
    sem_pix['_pix_cadastro'] = pix_lookup.apply(lambda x: x[0])
    sem_pix['_pix_tipo_cadastro'] = pix_lookup.apply(lambda x: x[1])
    sem_pix['_pix_resolvido'] = sem_pix['_pix_cadastro'].notna()
    return sem_pix
 
 
def detectar_outros_problemas(df):
    """Outros sinais de alerta operacionais."""
    problemas = {}
    problemas['valor_zero'] = df[df['_valor_num'] == 0]
    problemas['valor_negativo'] = df[df['_valor_num'] < 0]
    problemas['sem_cliente'] = df[df['Cliente'].isna()]
    problemas['sem_codigo_tarefa'] = df[df['Código da tarefa'].isna() | (df['Código da tarefa'].astype(str).str.strip() == '')]
    return problemas
 
 
# ============================================================
# CONSTRUÇÃO DO ARQUIVO FINAL
# ============================================================
 
def numero_semana_iso(data):
    """Retorna 'Semana N' baseado na semana ISO."""
    if pd.isna(data):
        return ''
    return f'Semana {data.isocalendar().week}'
 
 
def montar_pagamentos(df_limpo, cadastro_df, semana_label, data_registro,
                       data_vencimento, departamento='Equipe externa',
                       overrides_pix=None):
    """
    Recebe DataFrame já auditado e gera DataFrame no formato final
    "Pagamentos - Equipe externa".
    """
    overrides_pix = overrides_pix or {}
    linhas = []
    for _, r in df_limpo.iterrows():
        cpf_fmt = r['_cpf_fmt']
        nome = str(r.get('Nome do profissional', '')).strip()
        fornecedor = f'{nome},{cpf_fmt}' if cpf_fmt else nome
 
        tipo_despesa = str(r.get('Tipo de Despesa', '')).strip()
        categoria = CATEGORIA_MAP.get(tipo_despesa, tipo_despesa)
 
        # Chave PIX: 1) override do usuário, 2) cadastro (se faltava), 3) original formatada
        pix_original = r.get('Chave PIX')
        tipo_pix = r.get('Tipo da Chave PIX')
        idx = r['_idx']
        if idx in overrides_pix and overrides_pix[idx]:
            pix_final = overrides_pix[idx]
        elif pd.isna(pix_original) or str(pix_original).strip() == '':
            # buscar no cadastro
            pix_cad, tipo_cad = lookup_pix_cadastro(r['_cpf_digits'], cadastro_df)
            pix_final = pix_cad if pix_cad else ''
        else:
            pix_final = formatar_pix(pix_original, tipo_pix)
 
        linhas.append({
            'Semanas': semana_label,
            'Fornecedor * (Razão Social, Nome Fantasia, CNPJ ou CPF)': fornecedor,
            'Categoria *': categoria,
            'Valor *': r['_valor_num'],
            'Data de Registro *': data_registro,
            'Data de Vencimento *': data_vencimento,
            'Observações': tipo_despesa,
            'Chave Pix': pix_final,
            'Departamento (100%)': departamento,
        })
    return pd.DataFrame(linhas)
 
 
# ============================================================
# CRUZAMENTO COM A ESCALA
# ============================================================
 
# Status_ee da escala que indicam que o profissional efetivamente trabalhou
# (e portanto deve receber diária)
STATUS_TRABALHADOS = [
    'Pronto P/ Faturamento',
    'Checkout Efetuado',
    'Em Execução',
    'Em validação',
]
 
# Tipos de despesa do relatório que correspondem a uma "diária"
# (1 diária por dia por profissional, independente de quantas tarefas)
TIPOS_DIARIA = [
    'Diária T + VT + VA',
    'Diária A + VT + VA',
    'Diária M + VT + VA',
    'Adicional de diária',
    'Ajuda de custo',
    'Ajuda de custo para Motorista',
]
 
 
def carregar_escala(arquivo):
    """
    Carrega arquivo de escala (Escala_fin.xlsx ou csv).
    Espera as colunas: cpf, nome_completo, inicio, status_ee, codigo, at_cliente.razao_social
    """
    nome = getattr(arquivo, 'name', str(arquivo)).lower()
    if nome.endswith('.csv'):
        df = pd.read_csv(arquivo, encoding='utf-8', sep=None, engine='python')
    else:
        df = pd.read_excel(arquivo)
 
    df.columns = [str(c).lstrip('\ufeff').strip() for c in df.columns]
 
    obrig = ['cpf', 'inicio', 'status_ee']
    faltam = [c for c in obrig if c not in df.columns]
    if faltam:
        raise ValueError(
            f'Arquivo de Escala está com colunas faltando: {", ".join(faltam)}.\n'
            f'Colunas encontradas: {", ".join(df.columns[:10])}...'
        )
 
    df['_cpf_digits'] = df['cpf'].apply(cpf_digits)
    # 'inicio' pode vir como string ISO UTC ou Timestamp
    df['_data'] = pd.to_datetime(df['inicio'], errors='coerce', utc=True)
    if df['_data'].notna().any():
        df['_data'] = df['_data'].dt.tz_localize(None)
    df['_data'] = df['_data'].dt.date
    return df
 
 
def cruzar_escala_pagamento(df_relatorio, df_escala, data_inicio, data_fim,
                             status_validos=None):
    """
    Faz o cruzamento entre escalas trabalhadas e diárias do relatório,
    no range [data_inicio, data_fim].
 
    Retorna dois DataFrames:
      - escalas_sem_diaria: profissional escalado/trabalhou no período mas
        não tem diária no relatório → pagamento faltando
      - diarias_sem_escala: diária no relatório mas sem escala válida no
        período → pagamento indevido (ou escala incompleta)
    """
    if status_validos is None:
        status_validos = STATUS_TRABALHADOS
 
    # 1) Escalas válidas no período
    esc = df_escala.copy()
    esc = esc[esc['status_ee'].isin(status_validos)]
    esc = esc[esc['_data'].notna()]
    esc = esc[(esc['_data'] >= data_inicio) & (esc['_data'] <= data_fim)]
    esc = esc[esc['_cpf_digits'] != '']
 
    # Reduzir para 1 linha por (CPF, data) — mantém info de tarefa/cliente p/ exibir
    esc_unica = esc.drop_duplicates(subset=['_cpf_digits', '_data'], keep='first').copy()
    chaves_escala = set(zip(esc_unica['_cpf_digits'], esc_unica['_data']))
 
    # 2) Diárias do relatório no período
    rel = df_relatorio.copy()
    rel = rel[rel['Tipo de Despesa'].isin(TIPOS_DIARIA)]
    rel = rel[rel['_data_despesa'].notna()]
    rel['_data_only'] = pd.to_datetime(rel['_data_despesa']).dt.date
    rel = rel[(rel['_data_only'] >= data_inicio) & (rel['_data_only'] <= data_fim)]
    rel = rel[rel['_cpf_digits'] != '']
 
    chaves_relatorio = set(zip(rel['_cpf_digits'], rel['_data_only']))
 
    # 3) Diferenças
    falta_pagar_keys = chaves_escala - chaves_relatorio
    pago_sem_escala_keys = chaves_relatorio - chaves_escala
 
    # 4) Escalas sem diária (faltando pagar)
    escalas_sem_diaria = esc_unica[
        esc_unica.apply(lambda r: (r['_cpf_digits'], r['_data']) in falta_pagar_keys, axis=1)
    ].copy()
 
    # 5) Diárias sem escala (pago indevido)
    diarias_sem_escala = rel[
        rel.apply(lambda r: (r['_cpf_digits'], r['_data_only']) in pago_sem_escala_keys, axis=1)
    ].copy()
 
    return escalas_sem_diaria, diarias_sem_escala
