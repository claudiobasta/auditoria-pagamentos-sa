"""
Auditoria de Pagamentos - Equipe Externa
S&A Imunizações
 
Pipeline:
1. Upload do relatório CSV + cadastro _EXT_ Profissional
2. Auditoria automática (duplicatas, CPF, PIX, sobreposição, etc.)
3. Revisão manual de suspeitas com checkboxes
4. Geração do xlsx final no formato do financeiro
"""
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from io import BytesIO
 
from auditoria import (
    carregar_relatorio, carregar_cadastro,
    detectar_duplicatas_exatas, detectar_duplicatas_suspeitas,
    detectar_sobreposicao_tarefas, detectar_cpfs_invalidos,
    detectar_pix_faltante, detectar_outros_problemas,
    montar_pagamentos, numero_semana_iso,
    formatar_pix, lookup_pix_cadastro,
)
from exportador import gerar_xlsx_financeiro
 
 
st.set_page_config(
    page_title='Auditoria Pagamentos - S&A',
    page_icon='💉',
    layout='wide',
)
 
# ============================================================
# ESTILO
# ============================================================
st.markdown("""
<style>
    .main .block-container {padding-top: 2rem;}
    .metric-card {background: #f0f2f6; padding: 1rem; border-radius: 8px;}
    div[data-testid="stMetricValue"] {font-size: 1.5rem;}
    .stAlert {padding: 0.5rem 1rem;}
</style>
""", unsafe_allow_html=True)
 
st.title('💉 Auditoria de Pagamentos — Equipe Externa')
st.caption('S&A Imunizações | Auditoria automática + revisão manual + exportação para o financeiro')
 
# ============================================================
# ESTADO
# ============================================================
if 'etapa' not in st.session_state:
    st.session_state.etapa = 1
if 'remover_indices' not in st.session_state:
    st.session_state.remover_indices = set()
if 'overrides_pix' not in st.session_state:
    st.session_state.overrides_pix = {}
 
 
# ============================================================
# SIDEBAR - UPLOADS E PARÂMETROS
# ============================================================
with st.sidebar:
    st.header('📂 Arquivos')
    arq_relatorio = st.file_uploader(
        'Relatório (CSV)',
        type=['csv'],
        help='CSV exportado do Bubble (ex.: Relatorio_AAAA_MM_DD.csv)',
    )
    arq_cadastro = st.file_uploader(
        'Cadastro _EXT_ Profissional (XLSX ou CSV)',
        type=['xlsx', 'xls', 'csv'],
        help='Versão mais recente do cadastro de profissionais externos.',
    )
 
    st.divider()
    st.header('⚙️ Parâmetros')
    hoje = date.today()
    data_registro = st.date_input('Data de Registro', value=hoje)
    data_vencimento = st.date_input(
        'Data de Vencimento',
        value=hoje + timedelta(days=2),
    )
    semana_auto = f'Semana {hoje.isocalendar().week}'
    semana_label = st.text_input('Semana', value=semana_auto)
    departamento = st.text_input('Departamento', value='Equipe externa')
 
    st.divider()
    if st.button('🔄 Resetar revisão', use_container_width=True):
        st.session_state.remover_indices = set()
        st.session_state.overrides_pix = {}
        st.rerun()
 
 
# ============================================================
# VALIDAÇÕES INICIAIS
# ============================================================
if not arq_relatorio or not arq_cadastro:
    st.info('👈 Comece subindo o **relatório** e o **cadastro _EXT_ Profissional** atualizado na barra lateral.')
    with st.expander('ℹ️ Como funciona o fluxo'):
        st.markdown("""
        1. **Upload** do relatório (CSV) e do cadastro de profissionais (sempre a versão mais recente).
        2. O app remove automaticamente **duplicatas exatas** (mesmo `id_despesa`).
        3. Apresenta para sua revisão:
           - **Duplicatas suspeitas** (mesmo CPF + tarefa + tipo + valor)
           - **CPFs inválidos**
           - **Sobreposição** (mesmo profissional em tarefas diferentes no mesmo dia)
           - **Chaves PIX faltantes** — busca automaticamente no cadastro
           - **Outros alertas** (valor zero/negativo, sem cliente, sem tarefa)
        4. Você marca o que quer remover e edita PIX manualmente se preciso.
        5. Gera o **xlsx final** já no formato exato do financeiro.
        """)
    st.stop()
 
 
# ============================================================
# CARREGAR DADOS
# ============================================================
try:
    df = carregar_relatorio(arq_relatorio)
    cadastro = carregar_cadastro(arq_cadastro)
except ValueError as e:
    st.error('❌ Problema com os arquivos enviados')
    st.markdown(str(e))
    st.info(
        '💡 **Dica:** Confira na barra lateral se você não trocou os arquivos de campo. '
        'O **Relatório** vem do Bubble e tem a coluna "Valor". '
        'O **Cadastro** é o `_EXT_ Profissional` e tem "Chave PIX" + "Número do CPF".'
    )
    st.stop()
except Exception as e:
    st.error(f'Erro inesperado ao carregar arquivos: {e}')
    st.stop()
 
# Auto-remoção de duplicatas exatas
indices_dup_exatas = detectar_duplicatas_exatas(df)
df_sem_dup_exatas = df.drop(index=indices_dup_exatas)
 
# Análises sobre o df já sem duplicatas exatas
suspeitas, grupos_suspeitos = detectar_duplicatas_suspeitas(df_sem_dup_exatas)
sobreposicao = detectar_sobreposicao_tarefas(df_sem_dup_exatas)
invalidos, sem_cpf = detectar_cpfs_invalidos(df_sem_dup_exatas)
sem_pix = detectar_pix_faltante(df_sem_dup_exatas, cadastro)
outros = detectar_outros_problemas(df_sem_dup_exatas)
 
pix_resolvidos_auto = int(sem_pix['_pix_resolvido'].sum()) if not sem_pix.empty else 0
pix_nao_resolvidos = len(sem_pix) - pix_resolvidos_auto
 
 
# ============================================================
# DASHBOARD
# ============================================================
st.subheader('📊 Visão Geral')
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric('Lançamentos', len(df))
c2.metric('Dup. exatas removidas', len(indices_dup_exatas))
c3.metric('Suspeitas p/ revisar', len(suspeitas))
c4.metric('Sem PIX (cadastro resolveu)', f'{pix_resolvidos_auto}/{len(sem_pix)}')
c5.metric('Valor total bruto', f'R$ {df["_valor_num"].sum():,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.'))
 
st.divider()
 
# ============================================================
# ABAS DE AUDITORIA
# ============================================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    f'⚠️ Duplicatas suspeitas ({len(suspeitas)})',
    f'🆔 CPF ({len(invalidos)} inv. / {len(sem_cpf)} sem)',
    f'🔑 PIX faltante ({pix_nao_resolvidos} pendentes)',
    f'📅 Sobreposição ({len(sobreposicao)})',
    f'🚨 Outros alertas',
    '👀 Pré-visualizar dados completos',
])
 
# ----- ABA 1: DUPLICATAS SUSPEITAS -----
with tab1:
    if suspeitas.empty:
        st.success('✅ Nenhuma duplicata suspeita encontrada.')
    else:
        st.markdown(
            'Estes são pares/grupos com **mesmo CPF + tarefa + tipo de despesa + valor**. '
            'Marque as linhas que você quer **remover** do pagamento.'
        )
 
        for grupo in grupos_suspeitos:
            grupo_df = suspeitas[suspeitas['_grupo_dup'] == grupo]
            if grupo_df.empty:
                continue
            primeira = grupo_df.iloc[0]
            nome = primeira.get('Nome do profissional', '—') or '—'
            tipo = primeira.get('Tipo de Despesa', '—') or '—'
            valor = primeira.get('_valor_num', 0) or 0
            with st.expander(
                f'#{grupo} — {nome} | {tipo} | R$ {valor:.2f} '
                f'({len(grupo_df)} lançamentos)'
            ):
                for _, linha in grupo_df.iterrows():
                    idx = linha['_idx']
                    cols = st.columns([1, 5])
                    marcado = cols[0].checkbox(
                        'Remover',
                        value=(idx in st.session_state.remover_indices),
                        key=f'rem_dup_{idx}',
                    )
                    if marcado:
                        st.session_state.remover_indices.add(idx)
                    else:
                        st.session_state.remover_indices.discard(idx)
 
                    cols[1].write(
                        f'**Tarefa:** `{linha["Código da tarefa"]}` | '
                        f'**Data:** {linha["Data da despesa"]} | '
                        f'**Cliente:** {linha.get("Cliente", "—")} | '
                        f'**ID:** `{linha["id_despesa"][-8:]}`'
                    )
 
# ----- ABA 2: CPF -----
with tab2:
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('**CPFs inválidos** (dígitos verificadores não batem)')
        if invalidos.empty:
            st.success('✅ Todos os CPFs são válidos.')
        else:
            st.dataframe(
                invalidos[['Nome do profissional', '_cpf_fmt', 'Tipo de Despesa', 'Valor']],
                use_container_width=True, hide_index=True,
                column_config={'_cpf_fmt': 'CPF'},
            )
    with col2:
        st.markdown('**Sem CPF informado**')
        if sem_cpf.empty:
            st.success('✅ Todos os lançamentos têm CPF.')
        else:
            st.dataframe(
                sem_cpf[['Nome do profissional', 'Tipo de Despesa', 'Valor']],
                use_container_width=True, hide_index=True,
            )
 
# ----- ABA 3: PIX FALTANTE -----
with tab3:
    if sem_pix.empty:
        st.success('✅ Todos os lançamentos têm Chave PIX no relatório.')
    else:
        st.markdown(
            f'**{pix_resolvidos_auto}** de {len(sem_pix)} resolvidos automaticamente pelo cadastro. '
            f'Os demais precisam de PIX manual (ou serão exportados em branco).'
        )
 
        # Resolvidos pelo cadastro
        resolvidos = sem_pix[sem_pix['_pix_resolvido']]
        if not resolvidos.empty:
            with st.expander(f'✅ Resolvidos pelo cadastro ({len(resolvidos)})'):
                st.dataframe(
                    resolvidos[['Nome do profissional', '_cpf_fmt', '_pix_cadastro', '_pix_tipo_cadastro']],
                    use_container_width=True, hide_index=True,
                    column_config={
                        '_cpf_fmt': 'CPF',
                        '_pix_cadastro': 'PIX (cadastro)',
                        '_pix_tipo_cadastro': 'Tipo',
                    },
                )
 
        # Não resolvidos - permitir override manual
        nao_resolvidos = sem_pix[~sem_pix['_pix_resolvido']]
        if not nao_resolvidos.empty:
            st.markdown(f'**🔴 Sem PIX nem no relatório nem no cadastro ({len(nao_resolvidos)})**')
            st.caption('Preencha manualmente abaixo (opcional). Sem preenchimento, vai vazio para o financeiro.')
            for _, linha in nao_resolvidos.iterrows():
                idx = linha['_idx']
                cols = st.columns([3, 2, 3])
                cols[0].write(f'**{linha["Nome do profissional"]}**')
                cols[1].write(f'CPF: {linha["_cpf_fmt"]}')
                pix_manual = cols[2].text_input(
                    'PIX manual',
                    value=st.session_state.overrides_pix.get(idx, ''),
                    key=f'pix_man_{idx}',
                    label_visibility='collapsed',
                    placeholder='Cole a chave PIX aqui',
                )
                if pix_manual:
                    st.session_state.overrides_pix[idx] = pix_manual
                elif idx in st.session_state.overrides_pix:
                    del st.session_state.overrides_pix[idx]
 
# ----- ABA 4: SOBREPOSIÇÃO -----
with tab4:
    if sobreposicao.empty:
        st.success('✅ Nenhuma sobreposição detectada (mesmo profissional em tarefas diferentes no mesmo dia).')
    else:
        st.markdown(
            'Profissionais com lançamentos em **tarefas diferentes na mesma data**. '
            'Pode ser legítimo (algumas funções permitem) ou erro de lançamento — confira.'
        )
        for (cpf, data), grupo in sobreposicao.groupby(['_cpf_digits', '_data_despesa']):
            if grupo.empty:
                continue
            nome = grupo['Nome do profissional'].iloc[0] if 'Nome do profissional' in grupo.columns else '—'
            data_str = pd.Timestamp(data).strftime('%d/%m/%Y') if pd.notna(data) else '—'
            with st.expander(f'{nome} — {data_str} ({grupo["Código da tarefa"].nunique()} tarefas)'):
                for _, linha in grupo.iterrows():
                    idx = linha['_idx']
                    cols = st.columns([1, 5])
                    marcado = cols[0].checkbox(
                        'Remover',
                        value=(idx in st.session_state.remover_indices),
                        key=f'rem_sob_{idx}',
                    )
                    if marcado:
                        st.session_state.remover_indices.add(idx)
                    else:
                        st.session_state.remover_indices.discard(idx)
                    cols[1].write(
                        f'`{linha["Código da tarefa"]}` | {linha["Tipo de Despesa"]} | '
                        f'R$ {linha["_valor_num"]:.2f} | Cliente: {linha.get("Cliente", "—")}'
                    )
 
# ----- ABA 5: OUTROS ALERTAS -----
with tab5:
    cols_show = ['Nome do profissional', 'Código da tarefa', 'Tipo de Despesa', 'Valor', 'Cliente']
 
    st.markdown('**Valor zerado**')
    if outros['valor_zero'].empty:
        st.success('Nenhum.')
    else:
        st.caption('⚠️ Geralmente "Deslocamento KM" zerado é normal — confira mesmo assim.')
        st.dataframe(outros['valor_zero'][cols_show], use_container_width=True, hide_index=True)
 
    st.markdown('**Valor negativo**')
    if outros['valor_negativo'].empty:
        st.success('Nenhum.')
    else:
        st.dataframe(outros['valor_negativo'][cols_show], use_container_width=True, hide_index=True)
 
    st.markdown('**Sem cliente**')
    if outros['sem_cliente'].empty:
        st.success('Nenhum.')
    else:
        st.dataframe(outros['sem_cliente'][cols_show], use_container_width=True, hide_index=True)
 
    st.markdown('**Sem código de tarefa**')
    if outros['sem_codigo_tarefa'].empty:
        st.success('Nenhum.')
    else:
        st.dataframe(outros['sem_codigo_tarefa'][cols_show], use_container_width=True, hide_index=True)
 
# ----- ABA 6: PRÉ-VISUALIZAÇÃO -----
with tab6:
    st.dataframe(
        df_sem_dup_exatas.drop(columns=[c for c in df_sem_dup_exatas.columns if c.startswith('_')]),
        use_container_width=True, hide_index=True,
    )
 
 
# ============================================================
# RESUMO FINAL E EXPORTAÇÃO
# ============================================================
st.divider()
st.subheader('📤 Geração do arquivo final')
 
# Aplicar remoções marcadas
df_final = df_sem_dup_exatas.drop(
    index=[i for i in st.session_state.remover_indices if i in df_sem_dup_exatas.index]
)
 
c1, c2, c3 = st.columns(3)
c1.metric('Lançamentos finais', len(df_final))
c2.metric('Marcados p/ remoção', len(st.session_state.remover_indices))
total_final = df_final['_valor_num'].sum()
c3.metric(
    'Valor total final',
    'R$ ' + f'{total_final:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')
)
 
# Avisos antes de gerar
if pix_nao_resolvidos > 0 and not st.session_state.overrides_pix:
    st.warning(
        f'⚠️ Existem **{pix_nao_resolvidos}** lançamentos sem PIX que não foram preenchidos manualmente. '
        'Eles serão exportados com PIX em branco. Volte na aba "PIX faltante" se quiser corrigir.'
    )
 
if st.button('✨ Gerar arquivo Pagamentos - Equipe Externa', type='primary', use_container_width=True):
    df_pagamentos = montar_pagamentos(
        df_limpo=df_final,
        cadastro_df=cadastro,
        semana_label=semana_label,
        data_registro=data_registro,
        data_vencimento=data_vencimento,
        departamento=departamento,
        overrides_pix=st.session_state.overrides_pix,
    )
 
    xlsx_bytes = gerar_xlsx_financeiro(df_pagamentos)
 
    st.success(f'✅ Arquivo gerado com **{len(df_pagamentos)}** lançamentos.')
    st.dataframe(df_pagamentos, use_container_width=True, hide_index=True)
 
    nome_arquivo = f'Pagamentos_Equipe_Externa_{date.today().strftime("%Y_%m_%d")}.xlsx'
    st.download_button(
        '⬇️ Baixar XLSX',
        data=xlsx_bytes,
        file_name=nome_arquivo,
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        type='primary',
        use_container_width=True,
    )
