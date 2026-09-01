# Noctcloud Desenvolvimento LTDA
# Copyright (C) 2026  Noctcloud Desenvolvimento LTDA

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import marimo

__generated_with = "0.23.15"
app = marimo.App()


@app.cell
def _(mo):
    mo.Html("""
    <style>
        body, .marimo, p, div, span, label, button, input, textarea {
            font-family: "Inter", "SF Pro", -apple-system, BlinkMacSystemFont, sans-serif !important;
        }
        code, pre, .monospace {
            font-family: "SF Mono", "JetBrains Mono", "Fira Code", monospace !important;
        }
    </style>
    """)
    return


@app.cell
def _():
    import asyncio
    import marimo as mo
    import duckdb
    import plotly.express as px
    import time

    # Altere para o caminho do seu banco
    con = duckdb.connect(
        "/Users/guilhermecastro/Documents/gmt-gateway-backend/despesas.duckdb"
    )

    con.execute("""
    CREATE TABLE IF NOT EXISTS despesas AS
    SELECT *
    FROM read_csv(
        '/Users/guilhermecastro/Downloads/dados_abertos_despesas_a_partir_2022.csv',
        header = true
    )
    """)

    texto_completo = "Quais programas receberam mais recursos em 2025?"

    return asyncio, con, mo, px, texto_completo, time


@app.cell
def _(mo):
    mo.md(
        """
        # Guia Aberto MG
        """
    )
    return


@app.cell
async def _(asyncio, con, mo, px, texto_completo, time):
    PAUSA_CURTA = 0.035
    PAUSA_LONGA = 0.20

    # Efeito de digitacao com pausas naturais
    digitado = ""
    for char in texto_completo:
        digitado += char
        mo.output.replace(mo.md(f"💬 `{digitado}_`"))
        if char in (" ", ",", "?"):
            await asyncio.sleep(PAUSA_LONGA)
        else:
            await asyncio.sleep(PAUSA_CURTA)

    await asyncio.sleep(0.5)

    # Transicao: entendendo a pergunta
    mo.output.append(mo.md("⏳ Entendendo a pergunta..."))
    await asyncio.sleep(0.5)

    mo.output.append(mo.md("🧠 Traduzindo para SQL..."))
    await asyncio.sleep(0.7)

    sql = """
    SELECT
        NOME_PROGRAMA,
        SUM(
            CAST(REPLACE(REPLACE(VALOR_ORDEM_PAGAMENTO,'.',''),',','.') AS DOUBLE)
        ) AS total_pago
    FROM despesas
    WHERE "#EXERCICIO" = 2025
    GROUP BY NOME_PROGRAMA
    ORDER BY total_pago DESC
    LIMIT 5;
    """

    # Limpa tudo e comeca a digitar o SQL
    mo.output.clear()
    mo.output.append(mo.md("🧠 Traduzindo para SQL..."))

    sql_linhas = sql.strip().split("\n")
    sql_digitado = ""
    for linha in sql_linhas:
        for char in linha:
            sql_digitado += char
            await asyncio.sleep(0.005)
        sql_digitado += "\n"
        mo.output.replace(
            mo.md(
                f"""🧠 Traduzindo para SQL...

```sql
{sql_digitado}_
```"""
            )
        )
        await asyncio.sleep(0.08)

    mo.output.replace(
        mo.md(
            f"""🧠 Traduzindo para SQL...

```sql
{sql}
```"""
        )
    )

    await asyncio.sleep(0.5)

    mo.output.append(mo.md("⚡ Executando consulta..."))

    inicio = time.time()
    df = con.sql(sql).df()
    elapsed_ms = int((time.time() - inicio) * 1000)

    # Formata legenda: "Programa - R$ 1.234.567"
    df["legenda"] = df.apply(
        lambda r: f"{r['NOME_PROGRAMA']} - R$ {r['total_pago']:,.0f}".replace(",", "."),
        axis=1,
    )

    # Limpa o SQL e mostra so o grafico
    mo.output.clear()
    mo.output.append(mo.md("⚡ Executando consulta..."))

    fig = px.bar(
        df,
        x="NOME_PROGRAMA",
        y="total_pago",
        color="NOME_PROGRAMA",
        title="Programas com maior volume de recursos em 2025",
    )

    # Legenda colorida com nome do programa + valor
    for i, trace in enumerate(fig.data):
        trace.name = df.iloc[i]["legenda"]

    fig.update_traces(marker_line_width=0)

    fig.update_layout(
        template="plotly_white",
        xaxis_title="",
        yaxis_title="",
        xaxis_showticklabels=False,
        showlegend=True,
        legend_title="",
        legend_x=1.02,
        legend_y=1,
        height=450,
        font_family="Inter, SF Pro, -apple-system, sans-serif",
    )

    mo.output.append(fig)

    mo.output.append(mo.md(f"✅ Consulta executada em **{elapsed_ms} ms**"))


if __name__ == "__main__":
    app.run()
