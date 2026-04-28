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
# NORMALIZADORES CANÔNICOS
# ------------------------------------------------------------
# Toda fonte de dados (relatório, cadastro, escala) passa pelos
# mesmos normalizadores antes de virar coluna auxiliar interna.
# Garantem que o mesmo conceito tenha o mesmo formato em todos
# os arquivos, mesmo quando vêm com tipos diferentes (ex.: CPF
# como float em notação científica vs string com pontuação).
#
# Convenção de nomes: normalizar_*(valor_bruto) -> valor_canônico.
# Funções legadas (cpf_digits, parse_valor, parse_data) viram
# aliases para preservar compatibilidade com o resto do módulo.
# ============================================================
 
 
def normalizar_cpf(valor):
    """
    Converte qualquer entrada que represente um CPF para a forma
    canônica: string com 11 dígitos, com zeros à esquerda preservados.
 
    Aceita: '12345678901', 12345678901, 1.234567e+10, '123.456.789-01',
            '04395616631', '   123.456.789-01   ', None, NaN.
    Retorna: string de 11 dígitos OU '' se não for possível normalizar.
 
    Comportamento crítico: se vier um float em notação científica
    (ex.: 4.395617e+09), converte via string mas alerta que pode haver
    perda de precisão. Para evitar isso, sempre leia colunas de CPF
    com dtype=str na origem.
    """
    if valor is None or pd.isna(valor):
        return ''
    # Se for float, converte cuidadosamente para evitar notação científica
    if isinstance(valor, float):
        # repr float perde dígitos, mas Int conversão direta é mais segura
        try:
            valor = str(int(valor))
        except (ValueError, OverflowError):
            valor = repr(valor)
    s = ''.join(ch for ch in str(valor) if ch.isdigit())
    if not s:
        return ''
    # CPFs com até 11 dígitos: completa zeros à esquerda
    # CPFs com mais que 11 (provável corrupção): pega os 11 últimos
    if len(s) <= 11:
        return s.zfill(11)
    return s[-11:]
 
 
def formatar_cpf(valor):
    """Devolve CPF no formato visual 000.000.000-00 (ou '' se inválido)."""
    s = normalizar_cpf(valor)
    if len(s) != 11:
        return ''
    return f'{s[:3]}.{s[3:6]}.{s[6:9]}-{s[9:]}'
 
 
def validar_cpf(valor):
    """Valida CPF pelos dígitos verificadores. Retorna bool."""
    s = normalizar_cpf(valor)
    if len(s) != 11 or s == s[0] * 11:
        return False
    soma = sum(int(s[i]) * (10 - i) for i in range(9))
    d1 = (soma * 10 % 11) % 10
    if d1 != int(s[9]):
        return False
    soma = sum(int(s[i]) * (11 - i) for i in range(10))
    d2 = (soma * 10 % 11) % 10
    return d2 == int(s[10])
 
 
def normalizar_valor(valor):
    """
    Converte qualquer representação monetária para float em reais.
 
    Aceita: 'R$ 1.234,56', '1234.56', '1.234,56', 1234.56, '1234', None.
    Retorna: float (0.0 se não for possível parsear).
    """
    if valor is None or pd.isna(valor):
        return 0.0
    if isinstance(valor, (int, float)):
        return float(valor)
    s = str(valor).replace('R$', '').replace(' ', '').strip()
    if not s:
        return 0.0
    # Heurística pt-BR: se há vírgula, ela é decimal e ponto é milhar
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0
 
 
def normalizar_data(valor):
    """
    Converte qualquer representação de data para um objeto datetime.date
    (sem hora, sem timezone). Retorna None se não for parseável.
 
    Aceita: '25/04/2026', '2026-04-25T13:00:00.000Z', Timestamp(...),
            datetime(...), '2026-04-25 09:30:00', None.
    """
    if valor is None or (not isinstance(valor, str) and pd.isna(valor)):
        return None
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, pd.Timestamp):
        if valor.tzinfo is not None:
            valor = valor.tz_convert(None) if valor.tzinfo else valor.tz_localize(None)
        return valor.date()
    # String: tentar com timezone primeiro (formato ISO UTC), depois dayfirst
    try:
        ts = pd.to_datetime(valor, errors='coerce', utc=True)
        if pd.isna(ts):
            ts = pd.to_datetime(valor, errors='coerce', dayfirst=True)
        if pd.isna(ts):
            return None
        if hasattr(ts, 'tz_localize') and ts.tzinfo is not None:
            ts = ts.tz_localize(None) if ts.tz is None else ts.tz_convert(None)
        return ts.date() if hasattr(ts, 'date') else None
    except Exception:
        return None
 
 
def normalizar_texto(valor):
    """
    Limpa string: trim + colapsa espaços múltiplos. Mantém capitalização.
    Útil para nomes, clientes, observações.
    """
    if valor is None or pd.isna(valor):
        return ''
    return ' '.join(str(valor).split())
 
 
def normalizar_chave(valor):
    """
    Normaliza um texto para servir de CHAVE de comparação:
    minúsculas + sem acentos + sem espaços extras + trim.
 
    Útil para comparar status, tipos, categorias sem quebrar por
    diferença de capitalização ou acentuação.
    """
    if valor is None or pd.isna(valor):
        return ''
    s = ' '.join(str(valor).split()).lower()
    # Remove acentos via NFD
    import unicodedata
    s = unicodedata.normalize('NFD', s)
    s = ''.join(ch for ch in s if unicodedata.category(ch) != 'Mn')
    return s
 
 
def formatar_pix(valor, tipo):
    """
    Formata a chave PIX de acordo com o tipo:
    - Telefone/Celular: +55 + dígitos (ex.: +5517996720550)
    - CPF/CNPJ: 000.000.000-00
    - Email/Aleatória: mantém como está
    """
    if valor is None or pd.isna(valor) or str(valor).strip() == '':
        return ''
    valor_str = str(valor).strip()
    tipo_norm = normalizar_chave(tipo)
 
    if 'telefone' in tipo_norm or 'celular' in tipo_norm:
        digits = ''.join(ch for ch in valor_str if ch.isdigit())
        if not digits:
            return valor_str
        if digits.startswith('55') and len(digits) in (12, 13):
            return f'+{digits}'
        return f'+55{digits}'
    if 'cpf' in tipo_norm or 'cnpj' in tipo_norm:
        digits = ''.join(ch for ch in valor_str if ch.isdigit())
        if len(digits) == 11:
            return formatar_cpf(digits)
        return valor_str
    return valor_str  # email, aleatória
 
 
# ------------------------------------------------------------
# Aliases legados (preservam compatibilidade com chamadas antigas)
# ------------------------------------------------------------
cpf_digits = normalizar_cpf
parse_valor = normalizar_valor
 
 
def parse_data(s):
    """Alias legado de normalizar_data, mas retorna pd.Timestamp/NaT."""
    d = normalizar_data(s)
    return pd.Timestamp(d) if d is not None else pd.NaT
 
 
# ============================================================
# CARGA
# ============================================================
 
RELATORIO_COLS_OBRIG = ['Valor', 'CPF', 'Nome do profissional', 'Tipo de Despesa',
                        'Código da tarefa', 'Data da despesa', 'id_despesa']
 
CADASTRO_COLS_OBRIG = ['Chave PIX', 'Número do CPF', 'Tipo de chave PIX']
 
 
def _normalizar_nomes_colunas(df):
    """Tira BOM, espaços extras e duplica nomes para acesso seguro."""
    df.columns = [' '.join(str(c).lstrip('\ufeff').split()) for c in df.columns]
    return df
 
 
def _ler_arquivo_tabular(arquivo, sheet_name=None, dtype_str_cols=None):
    """
    Lê um arquivo xlsx OU csv e devolve DataFrame com nomes de coluna
    normalizados (sem espaços extras, sem BOM).
 
    Para xlsx, tenta ler a sheet específica se fornecida; senão usa a primeira.
    Para csv, tenta encodings comuns.
 
    dtype_str_cols: lista opcional de colunas a forçar como string
                    (útil para CPF, códigos longos com zeros à esquerda, etc.).
    """
    nome = (getattr(arquivo, 'name', '') or str(arquivo)).lower()
    is_excel = nome.endswith(('.xlsx', '.xls', '.xlsm'))
 
    if is_excel:
        if hasattr(arquivo, 'seek'):
            arquivo.seek(0)
        kwargs = {}
        if dtype_str_cols:
            kwargs['dtype'] = {c: str for c in dtype_str_cols}
        if sheet_name is not None:
            try:
                df = pd.read_excel(arquivo, sheet_name=sheet_name, **kwargs)
            except Exception:
                if hasattr(arquivo, 'seek'):
                    arquivo.seek(0)
                df = pd.read_excel(arquivo, **kwargs)
        else:
            df = pd.read_excel(arquivo, **kwargs)
        return _normalizar_nomes_colunas(df)
 
    # CSV
    for enc in ('utf-8-sig', 'utf-8', 'latin-1', 'cp1252'):
        try:
            if hasattr(arquivo, 'seek'):
                arquivo.seek(0)
            kwargs = {'encoding': enc, 'sep': None, 'engine': 'python'}
            if dtype_str_cols:
                kwargs['dtype'] = {c: str for c in dtype_str_cols}
            else:
                # CSV sem hint: força tudo como string para preservar CPFs
                kwargs['dtype'] = str
            df = pd.read_csv(arquivo, **kwargs)
            return _normalizar_nomes_colunas(df)
        except (UnicodeDecodeError, UnicodeError):
            continue
    raise ValueError('Não foi possível ler o arquivo (encoding/formato não suportado).')
 
 
# Alias para compatibilidade com chamadas antigas
def _ler_csv_robusto(arquivo):
    return _ler_arquivo_tabular(arquivo)
 
 
def carregar_relatorio(arquivo):
    """
    Carrega o relatório de despesas (xlsx ou csv) e adiciona colunas
    auxiliares já normalizadas:
      _valor_num   -> float
      _cpf_digits  -> string com 11 dígitos
      _cpf_fmt     -> '000.000.000-00'
      _data_despesa-> datetime.date (sem hora)
      _idx         -> índice estável para revisão na UI
    """
    df = _ler_arquivo_tabular(arquivo, dtype_str_cols=['CPF', 'id_despesa', 'Código da tarefa'])
 
    faltam = [c for c in RELATORIO_COLS_OBRIG if c not in df.columns]
    if faltam:
        raise ValueError(
            f'Arquivo do Relatório está com colunas faltando: {", ".join(faltam)}.\n\n'
            f'Colunas encontradas: {", ".join(df.columns[:8])}...\n\n'
            f'Verifique se você subiu o relatório correto no campo "Relatório" '
            f'(o arquivo exportado do Bubble com despesas pendentes de pagamento).'
        )
 
    df['_valor_num'] = df['Valor'].apply(normalizar_valor)
    df['_cpf_digits'] = df['CPF'].apply(normalizar_cpf)
    df['_cpf_fmt'] = df['CPF'].apply(formatar_cpf)
    df['_data_despesa'] = df['Data da despesa'].apply(normalizar_data)
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
            df = pd.read_excel(arquivo, sheet_name='_EXT_ Profissional', dtype=str)
        except Exception:
            df = pd.read_excel(arquivo, dtype=str)
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
 
    Consolidação: lançamentos do mesmo profissional + mesmo Tipo de Despesa
    são somados em uma única linha. Ex.: 3 estacionamentos da semana
    (R$70 + R$70 + R$30) viram 1 linha de R$170.
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
 
    df_pag = pd.DataFrame(linhas)
    if df_pag.empty:
        return df_pag
 
    # ========== CONSOLIDAÇÃO ==========
    # Soma valores por (Fornecedor + Observações). Categoria, PIX, datas e
    # demais campos são iguais dentro do grupo (mesmo profissional + mesmo
    # tipo de despesa), então 'first' é seguro.
    chave = [
        'Fornecedor * (Razão Social, Nome Fantasia, CNPJ ou CPF)',
        'Observações',
    ]
    agg = {
        'Semanas': 'first',
        'Categoria *': 'first',
        'Valor *': 'sum',
        'Data de Registro *': 'first',
        'Data de Vencimento *': 'first',
        'Chave Pix': 'first',
        'Departamento (100%)': 'first',
    }
    df_pag = df_pag.groupby(chave, as_index=False, sort=False).agg(agg)
 
    # Reordena colunas para ficarem na mesma ordem do template do financeiro
    ordem = [
        'Semanas',
        'Fornecedor * (Razão Social, Nome Fantasia, CNPJ ou CPF)',
        'Categoria *',
        'Valor *',
        'Data de Registro *',
        'Data de Vencimento *',
        'Observações',
        'Chave Pix',
        'Departamento (100%)',
    ]
    return df_pag[ordem]
 
 
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
    # Lê o CPF como string para preservar zeros à esquerda. Demais colunas livres.
    if nome.endswith('.csv'):
        df = pd.read_csv(arquivo, encoding='utf-8', sep=None, engine='python', dtype={'cpf': str})
    else:
        df = pd.read_excel(arquivo, dtype={'cpf': str})
 
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
