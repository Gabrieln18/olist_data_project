import streamlit as st
import duckdb
import pandas as pd
import requests
import json
import plotly.express as px
import plotly.graph_objects as go
import altair as alt


# ---------- FUNÇÃO DE CONSULTA ----------
def execute_query(query: str) -> pd.DataFrame:
    conn = duckdb.connect()
    result = conn.sql(query).fetchdf()
    conn.close()
    return result

def load_data():
    """Carrega todas as tabelas em um dicionário"""
    base_path = "/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/"

    return {
        "seller_perf": execute_query(f"SELECT * FROM delta_scan('{base_path}seller_performance')"),
        "cancellation": execute_query(f"SELECT * FROM delta_scan('{base_path}agg_sellers_cancellation_rate_v2')"),
        "reviews": execute_query(f"SELECT * FROM delta_scan('{base_path}gold_sellers_order_reviews_score')"),
        "shipments": execute_query(f"SELECT * FROM delta_scan('{base_path}gold_sellers_shipment_metrics')")
    }

def get_seller_data(data: dict, seller_id: str):
    """Filtra os dados de todas as tabelas para um vendedor específico"""
    return {
        name: df[df["seller_id"] == seller_id]
        for name, df in data.items()
    }

# ========== CONFIG DO APP ==========
st.set_page_config(page_title="Dashboard de Vendedores", page_icon="👨‍💼", layout="wide")
st.title("👨‍💼 Dashboard de Vendedores")
st.caption("Acompanhe a performance, risco, avaliações e logística dos seus vendedores.")

# ========== CARREGAMENTO DOS DADOS ==========
data = load_data()

# ========== SELEÇÃO DO VENDEDOR ==========
seller_ids = data["shipments"]["seller_id"].unique()
selected_seller = st.selectbox("Selecione um vendedor pelo ID", seller_ids)

# Filtra os dados do vendedor selecionado
seller = get_seller_data(data, selected_seller)

# ========== SEÇÃO 1: VISÃO GERAL ==========
st.subheader("🎯 Visão Geral do Vendedor")

# 🎯  Visão Geral do Seller

#     Total de Vendas (total_sales)
#     Receita Total (total_revenue)
#     Estado e Cidade (seller_state, seller_city)
#     Tempo de Atividade (dias) (seller_lifetime_days)
#     Nível de Risco (risk_level)
#     Pontuação de Risco Total (total_risk_score)

if not seller["shipments"].empty:
    row = seller["shipments"].iloc[0]

    with st.container(border=True):
        col1, col2, col3 = st.columns(3)

        with col1:
            st.write(f"**Vendedor ID**: :green[{selected_seller}]")
        with col2:
            st.write(f"**Cidade**: {row['seller_city']}")
        with col3:
            st.write(f"**Estado**: {row['seller_state']}")

with st.container(key="visao-geral-container-metricas"):
    col1, col2, col3, col4= st.columns(4)

    try:
        total_revenue = seller["seller_perf"]["total_revenue"].values[0]
        total_sales = seller["seller_perf"]["total_sales"].values[0]
        lifetime = seller["cancellation"]["seller_lifetime_days"].values[0]
        risk_level = seller["cancellation"]["risk_level"].values[0]
        # risk_total = seller["cancellation"]["total_risk_score"].values[0]

        with col1:
            st.metric("Total de Receita", f"R$ {total_revenue:,.2f}", help="Total de receitas realizadas durante o período de 2016 a 2018.")
        with col2:
            st.metric("Total de Vendas", total_sales, help="")
        with col3:
            st.metric("Tempo de Atividade", f"{lifetime} dias", help="Tempo de atividade do vendedor dentro da plataforma, desde o primeiro registro.")
        with col4:
            st.metric("Nível de Risco", f"{risk_level}", help="Nível de risco de cancelamento deste vendedor.")

    except IndexError:
        st.warning("Alguns dados não estão disponíveis para este vendedor.")

# ========== SEÇÃO 2: DESEMPENHO OPERACIONAL ==========
st.subheader("📦 Desempenho Operacional")

# 📦 Desempenho Operacional
#     Taxa de Cancelamento (cancellation_rate)
#     Pedidos Cancelados vs. Totais (canceled_orders / total_orders)
#     Score de Cancelamento (score_cancellation)
#     Atrasos em Entregas (% e dias médios) (delayed_rate_percent, avg_delay_days)
#     Score de Atraso (score_delay)
#     Último Pedido Atrasado (last_order_delayed)

with st.container(key="cancelamentos-operacional"):
    col1, col2, col3 = st.columns(3)

    try:
        df = seller["cancellation"]
        cancellation_rate = df["cancellation_rate"].values[0]
        canceled_orders = df["canceled_orders"].values[0]
        total_reviews = df["total_orders"].values[0]
        canceled_score = df["score_cancellation"].values[0]

        with col1:
            st.metric("Taxa de Cancelamentos", value=f"{cancellation_rate * 100:.2f}%", border=True, help="Porcentagem de cancelamento comparado ao total de pedidos.")
        with col2:
            st.metric("Pedidos Cancelados / Total", value=f"{canceled_orders} / {total_reviews}", border=True, help="Pedidos cancelados comparado ao total de pedidos realizados.")
        with col3:
            st.metric("Score de Cancelamento", value=f"{canceled_score}", border=True, help="Indicador de confiança baseado no risco de cancelamento deste vendedor.")

    except IndexError:
        st.warning("Alguns dados não estão disponíveis para este vendedor.")

with st.container(key="atrasos-operacional"):
    col1, col2, col3 = st.columns(3)

    try:
        df = seller["cancellation"]
        df_shipment = seller["shipments"]
        avg_delivery_delay = float(df["avg_delivery_delay"].values[0])
        score_delay = df["score_delay"].values[0]
        last_order_delayed = df_shipment["last_order_delayed"].values[0]

        with col1:
            st.metric("Atrasos de Entrega (%)", value=f"{round(avg_delivery_delay, 2)}%", border=True)
        with col2:
            st.metric("Score de Atraso", value=score_delay, border=True, help="Indicador de confiança baseado no risco de atraso deste vendedor.")

    except IndexError:
        st.warning("Alguns dados não estão disponíveis para este vendedor.")

with st.container(key="atrasos-operacional-ultimo-pedido",border=True):
    formatted_date = pd.to_datetime(last_order_delayed).strftime("%d/%m/%Y %H:%M")
    st.write(f"Último Pedido Atrasado em: :blue[{formatted_date}]")

st.divider()

# ========== SEÇÃO 3: REPUTAÇÃO E AVALIAÇÕES ==========
st.subheader("🌟 Reputação e Avaliações")

# 🌟 Reputação e Avaliações
#     Avaliação Média (avg_score_review)
#     Taxa de Avaliações Negativas (negative_review_rate)
#     Total de Avaliações x Avaliações Negativas
#     Score de Avaliações (score_reviews)

# ========== SELEÇÃO DO VENDEDOR ==========
seller_ids = data["reviews"]["seller_id"].unique()
selected_seller_review = st.selectbox("Selecione um vendedor pelo ID", seller_ids)

data_reviews = load_data()
data_reviews_df = data_reviews['reviews']
data_cancellation = data['cancellation']

st.write(f"**Vendedor ID: :green[{selected_seller_review}]**")

with st.container(key="reputacao-avaliacoes"):
    col1, col2, col3 = st.columns(3)

    try:
        review_seller_id = data_reviews_df[data_reviews_df['seller_id'] == f'{selected_seller_review}']
        negative_rate = data_cancellation[data_cancellation['seller_id'] == selected_seller_review]
        total_reviews = data_cancellation[data_cancellation['seller_id'] == selected_seller_review]
        negative_reviews = data_cancellation[data_cancellation['seller_id'] == selected_seller_review]
        score_review = data_cancellation[data_cancellation['seller_id'] == selected_seller_review]

        with col1:
            st.metric("Avaliação Média", value=f"{review_seller_id['avg_score_review'].values[0]:.2f}", border=True)
        with col2:
            st.metric("Taxa de Avaliação Negativa", value=f"{negative_rate['negative_review_rate'].values[0]:.2f}", border=True)
        with col3:
            st.metric("Avaliações Negativas / Total de Avaliações", value=f"{negative_reviews['negative_reviews'].values[0]} / {total_reviews['total_reviews'].values[0]}", border=True)

    except IndexError:
        st.warning("Alguns dados não estão disponíveis para este vendedor.")

with st.container(key="reputacao-avaliacoes-score"):
    try:
        st.metric("Score de Avaliações", value=f"{score_review['score_reviews'].values[0]}", border=True, help="Pontuação de confiança baseado em avaliações dos consumidores.")

    except IndexError:
        st.warning("Alguns dados não estão disponíveis para este vendedor.")

# ========== SEÇÃO 4: ANÁLISE DE RISCO ==========
st.subheader("📊 Análise de Risco")

# 📊 Análise de Risco
#     Score de Inexperiência (score_inexperience)
#     Resumo dos Scores (gráfico de barras com os 4 scores: cancelamento, avaliações, atraso, inexperiência)


# Selectbox dedicado para a seção de risco
seller_ids_risk = data["cancellation"]["seller_id"].unique()
selected_seller_risk = st.selectbox("Selecione um vendedor para análise de risco", seller_ids_risk)

# Filtra os dados diretamente da tabela cancellation
risk_data = data["cancellation"]
seller_risk_row = risk_data[risk_data["seller_id"] == selected_seller_risk]

if not seller_risk_row.empty:
    row = seller_risk_row.iloc[0]

    with st.container(border=True):
        col1, col2= st.columns(2)

        with col1:
            st.write(f"**Vendedor ID**: :green[{selected_seller}]")
        with col2:
        # 1. Score de Inexperiência
            st.metric("Score de Inexperiência", row['score_inexperience'], help="Pontuação de confiança de inexperiência do vendedor, baseado no tempo de atividade na plataforma.")

    
    # 2. Gráfico de barras com os 4 scores de risco
    st.markdown("#### 🧮 Distribuição dos Scores de Risco")

    scores_dict = {
        "Cancelamento": row['score_cancellation'],
        "Avaliações": row['score_reviews'],
        "Atraso": row['score_delay'],
        "Inexperiência": row['score_inexperience']
    }

    # Montar DataFrame
    scores_chart_df = pd.DataFrame([
        {"Score": score, "Tipo": tipo}
        for tipo, score in scores_dict.items()
        if pd.notna(score)  # evita barras ausentes
    ])

    # Criar gráfico com Altair
    chart = alt.Chart(scores_chart_df).mark_bar().encode(
        x=alt.X('Tipo:N', title="Tipo de Score"),
        y=alt.Y('Score:Q', title="Pontuação", scale=alt.Scale(domain=[0, 5])), # escala do gráfico
        color=alt.Color('Tipo:N', legend=None)
    ).properties(width=500, height=300)

    st.altair_chart(chart, use_container_width=True)

else:
    st.warning("Dados de risco não disponíveis para este vendedor.")
