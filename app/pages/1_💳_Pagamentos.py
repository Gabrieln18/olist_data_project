import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import duckdb

st.set_page_config(page_title="Pagamentos", page_icon="💳", layout="wide")

st.title("💳 Pagamentos")
st.caption(
    """
    Dashboard de pagamentos da Olist Ecommerce
    """
)

# Função utilitária
def execute_query(query: str) -> pd.DataFrame:
    conn = duckdb.connect()
    result = conn.sql(query).fetchdf()
    conn.close()
    return result

# ========= SEÇÂO PAGAMENTOS KPIS =========
st.subheader("Pagamentos")
st.caption("Painel de performance de pagamentos")

with st.container(key="kpis-pagamento"):
    col1, col2, col3, col4 = st.columns(4)

    df_pagamentos = execute_query("""
        SELECT * FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_payment_performance');
    """)
    # st.write(df_pagamentos)

    # top metodos consulta
    top_method_query = """
        SELECT 
            payment_type,
            total_orders,
            (total_orders * 100.0 / (SELECT SUM(total_orders) 
            FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_payment_performance'))) as percentage
        FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_payment_performance')
        ORDER BY total_orders DESC
        LIMIT 1
    """
    top_method = execute_query(top_method_query)

    # valores principais
    total_orders = int(df_pagamentos['total_orders'].sum())
    avg_payment_value = float(df_pagamentos['avg_payment_value'][0])  # Já vem da agregação da Gold Layer
    avg_order_value = float(df_pagamentos['avg_order_value'][0])

    # dados de metodos mais usados
    top_method_name = top_method['payment_type'][0]
    top_method_pct = float(top_method['percentage'][0]) / 100


    with col1:
        st.metric("Total de Pedidos", f"{total_orders:,}".replace(',', '.'), border=True)

    with col2:
        st.metric("Método Mais Utilizado", top_method_name, f"{top_method_pct:.1%}", border=True)

    with col3:
        st.metric("Valor Médio de Pagamento", f"R$ {avg_payment_value:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'), border=True)

    with col4:
        st.metric("Valor Médio de Pedido", f"R$ {avg_order_value:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'), border=True)

    

st.divider()

# ========= SEÇÂO DISTRIBUICAO DE PEDIDOS =========

with st.container(key="distribuicao-pedidos"):
    col1, col2 = st.columns(2)

    df_donut = execute_query("SELECT * FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_payment_performance');")

    methods = {
        'credit_card': 'Cartão de Crédito',
        'debit_card': 'Cartão de Débito',
        'voucher': 'Voucher',
        'boleto': 'Boleto'
    }
    df_donut['payment_name'] = df_donut['payment_type'].map(methods)
    df_donut = df_donut[df_donut['payment_name'].notna()]

    # Criação do gráfico de donut
    donut_chart = px.pie(
        data_frame=df_donut,
        values='total_orders',       # <- ajuste para o nome correto da sua coluna de contagem
        names='payment_name',
        hole=0.4,
        title="Distribuição de Pedidos por Método de Pagamento"
    )

    # Customizações visuais
    donut_chart.update_traces(
        textinfo='percent+label',
        textfont_size=14,
        marker=dict(line=dict(color='#000000', width=1))
    )

    donut_chart.update_layout(
        title=dict(
            font=dict(size=15)
        ),
        showlegend=False,
        margin=dict(t=50, b=0, l=0, r=0)
    )

    # valor médio por metodo de pagamento
    # Criar gráfico de barras agrupadas com Plotly
    payment_names = {
        'credit_card': 'Cartão de Crédito',
        'debit_card': 'Cartão de Débito',
        'voucher': 'Voucher',
        'boleto': 'Boleto'
    }

    # Adicionar coluna com nomes amigáveis
    df = execute_query("SELECT * FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_payment_performance');")
    df['payment_name'] = df['payment_type'].map(payment_names)
    bar_compared = go.Figure()

    # Adicionar barras para valor médio de pagamento
    bar_compared.add_trace(go.Bar(
        x=df['payment_name'],
        y=df['avg_payment_value'],
        name='Valor Médio de Pagamento',
        marker_color='#4e79a7',
        text=df['avg_payment_value'].apply(lambda x: f'R$ {x:.2f}'),
        textposition='outside',
        textangle=0
    ))

    # Adicionar barras para valor médio de pedido
    bar_compared.add_trace(go.Bar(
        x=df['payment_name'],
        y=df['avg_order_value'],
        name='Valor Médio de Pedido',
        marker_color='#76b7b2',
        text=df['avg_order_value'].apply(lambda x: f'R$ {x:.2f}'),
        textposition='outside',
        textangle=0
    ))

    # Atualizar layout do gráfico
    bar_compared.update_layout(
        title='Valor Médio por Método de Pagamento',
        title_font_size=18,
        xaxis_title='Método de Pagamento',
        yaxis_title='Valor (R$)',
        barmode='group',
        height=500,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        template='plotly_white'
    )

    with col1:
        st.plotly_chart(donut_chart, use_container_width=True)

    with col2:
        st.plotly_chart(bar_compared, use_container_width=True)

st.divider()

# ========= SEÇÂO DETALHAMENTO POR METODO DE PAGAMENTO =========

st.subheader("Detalhamento por Método de Pagamento")
with st.container(key="detalhamento-metodo-pagamento"):

    payment_names = {
        'credit_card': 'Cartão de Crédito',
        'debit_card': 'Cartão de Débito',
        'voucher': 'Voucher',
        'boleto': 'Boleto'
    }

    # consultas de tabelas ao duckdb
    table_df = execute_query("SELECT * FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_payment_performance');")
    table_df['payment_name'] = table_df['payment_type'].map(payment_names)
    table_df = table_df[['payment_name', 'total_orders', 'avg_payment_value', 'avg_order_value', 'cancellation_rate']]
    table_df.columns = ['Método de Pagamento', 'Total de Pedidos', 'Valor Médio de Pagamento (R$)', 
                        'Valor Médio de Pedido (R$)', 'Taxa de Cancelamento (%)']
    
    # Formatar valores para exibição
    table_df['Valor Médio de Pagamento (R$)'] = table_df['Valor Médio de Pagamento (R$)'].apply(lambda x: f'R$ {x:.2f}')
    table_df['Valor Médio de Pedido (R$)'] = table_df['Valor Médio de Pedido (R$)'].apply(lambda x: f'R$ {x:.2f}')
    table_df['Taxa de Cancelamento (%)'] = table_df['Taxa de Cancelamento (%)'].apply(lambda x: f'{x:.1f}%')

    st.dataframe(table_df, use_container_width=True)
        
        