# Auditoria de Pagamentos — Equipe Externa

Ferramenta para auditar o relatório de pagamentos da equipe externa e gerar o arquivo final no formato exigido pelo financeiro interno da S&A.

## O que faz

1. **Carrega** o relatório CSV (Bubble) e o cadastro `_EXT_ Profissional` atualizado.
2. **Auditoria automática:**
   - Remove duplicatas exatas (mesmo `id_despesa`).
   - Sinaliza duplicatas suspeitas (mesmo CPF + tarefa + tipo de despesa + valor).
   - Valida CPFs pelos dígitos verificadores.
   - Detecta sobreposição (profissional em tarefas diferentes na mesma data).
   - Cruza chaves PIX faltantes com o cadastro.
   - Lista valores zerados/negativos, lançamentos sem cliente/tarefa.
3. **Revisão manual** com checkboxes para aprovar remoções e editar PIX.
4. **Exporta** xlsx no formato exato do `Pagamentos - Equipe externa` (mesma fonte, cores, formatação de moeda BR, larguras de coluna).

## Instalação (uma vez só)

```bash
pip install -r requirements.txt
```

## Como rodar

Dentro da pasta do projeto:

```bash
streamlit run app.py
```

O navegador abre automaticamente em `http://localhost:8501`.

## Fluxo de uso

1. Suba o **relatório CSV** (do Bubble).
2. Suba o **cadastro `_EXT_ Profissional`** mais recente (xlsx ou csv).
3. Ajuste **Semana / Data de Registro / Data de Vencimento** na barra lateral.
4. Revise as abas:
   - ⚠️ **Duplicatas suspeitas** — marque o que remover.
   - 🆔 **CPF** — valida e lista os problemáticos.
   - 🔑 **PIX faltante** — preencha manualmente os que o cadastro não resolveu.
   - 📅 **Sobreposição** — confira e marque o que remover.
   - 🚨 **Outros alertas** — valor zero/negativo, sem cliente, sem tarefa.
5. Clique em **Gerar arquivo Pagamentos - Equipe Externa** e baixe o xlsx.

## Estrutura

- `app.py` — interface Streamlit
- `auditoria.py` — regras de auditoria e transformação
- `exportador.py` — geração do xlsx final formatado
