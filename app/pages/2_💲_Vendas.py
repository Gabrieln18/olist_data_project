import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import datetime

# Função utilitária
def execute_query(query: str) -> pd.DataFrame:
    conn = duckdb.connect()
    result = conn.sql(query).fetchdf()
    conn.close()
    return result

# Layout e título
st.set_page_config(page_title="Dashboard de Vendas", page_icon="💲", layout="wide")
st.title("💲 Dashboard de Vendas")
st.sidebar.success("Selecione uma página acima.")

# -----------------------
# Filtros Globais
# -----------------------

# Datas padrão
hoje = datetime.date.today()
inicio_padrao = datetime.date(2016, 1, 1)
fim_padrao = datetime.date(2025, 1, 31)

# Filtro de data
data_inicio, data_fim = st.date_input(
    "📅 Intervalo de Datas",
    (inicio_padrao, fim_padrao),
    min_value=datetime.date(2016, 1, 1),
    max_value=datetime.date(2025, 12, 31),
    format="DD/MM/YYYY"
)

# Consulta para pegar os dados com filtros aplicados
query_geo = f"""
    SELECT 
        customer_city, 
        customer_state, 
        total_revenue, 
        total_orders
    FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/geo_sales')
    LIMIT 10;
"""
geo_sales_df = execute_query(query_geo)

# UF e Cidade dinâmicas
ufs = ["Todos"] + sorted(geo_sales_df["customer_state"].unique())
uf_selecionada = st.selectbox("UF", ufs)

if uf_selecionada != "Todos":
    cidades = ["Todas"] + sorted(geo_sales_df[geo_sales_df["customer_state"] == uf_selecionada]["customer_city"].unique())
else:
    cidades = ["Todas"] + sorted(geo_sales_df["customer_city"].unique())

cidade_selecionada = st.selectbox("Cidade", cidades)

# -----------------------
# Aplicação de filtros no DataFrame
# -----------------------
df_filtrado = geo_sales_df.copy()

if uf_selecionada != "Todos":
    df_filtrado = df_filtrado[df_filtrado["customer_state"] == uf_selecionada]

if cidade_selecionada != "Todas":
    df_filtrado = df_filtrado[df_filtrado["customer_city"] == cidade_selecionada]

# -----------------------
# KPIs
# -----------------------
receita_total = df_filtrado["total_revenue"].sum()
qtd_total_pedidos = df_filtrado["total_orders"].sum()
ticket_medio = receita_total / qtd_total_pedidos if qtd_total_pedidos else 0

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("💰 Receita Total", f"R$ {receita_total:,.2f}")
with col2:
    st.metric("📦 Total de Pedidos", f"{qtd_total_pedidos:,}")
with col3:
    st.metric("🎫 Ticket Médio", f"R$ {ticket_medio:,.2f}")

st.divider()

# -----------------------
# Tabela Ranking de Vendas
# -----------------------
st.subheader("🥇 Ranking de Vendas")
st.text("Tabela ranqueada de vendas (ordenado por receita).")

# Ordenar por receita (decrescente)
ranking_df = df_filtrado.sort_values(by="total_revenue", ascending=False).reset_index(drop=True)

# Adicionar coluna de posição
ranking_df.insert(0, "Posição", ranking_df.index + 1)

# Adicionar emojis para o top 3
medals = {1: "🥇", 2: "🥈", 3: "🥉"}
ranking_df["Posição"] = ranking_df["Posição"].apply(lambda x: f"{medals[x]} {x}" if x in medals else f"{x}")

# Arredondar a coluna de receita (opcional)
ranking_df["total_revenue"] = ranking_df["total_revenue"].round(2)

# Exibir a tabela
st.dataframe(ranking_df, use_container_width=True, hide_index=True)

# -----------------------
# Gráfico: Vendas por Categoria
# -----------------------
st.subheader("📊 Receita por Categoria de Produto")

query_cat = f"""
    SELECT 
        product_category, 
        total_revenue, 
        total_quantity_sold, 
        average_price_per_item
    FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/sales_by_category')
    LIMIT 10;
"""
cat_df = execute_query(query_cat)

fig_cat = px.bar(
    cat_df.sort_values("total_revenue", ascending=False).round(2),
    x="product_category",
    y="total_revenue",
    color="total_revenue",
    title="Receita por Categoria",
    labels={"total_revenue": "Receita (R$)", "product_category": "Categoria"},
)
with st.container(border=True):
    st.plotly_chart(fig_cat, use_container_width=True)

# -----------------------
# Tabela Detalhada
# # -----------------------
# st.subheader("📍 Vendas por Localidade (Tabela)")
# st.dataframe(df_filtrado.sort_values(by="total_revenue", ascending=False), use_container_width=True)
