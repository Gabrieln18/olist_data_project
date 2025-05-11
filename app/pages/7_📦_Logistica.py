import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime

# Função utilitária
def execute_query(query: str) -> pd.DataFrame:
    conn = duckdb.connect()
    result = conn.sql(query).fetchdf()
    conn.close()
    return result

# Layout e título
st.set_page_config(page_title="Dashboard Logística", page_icon="📦", layout="wide")
st.title("📦 Dashboard de Análise Logística")

# Sidebar para filtros
st.sidebar.header("Filtros")

# Carregar dados das tabelas
@st.cache_data
def load_data():
    # Carregar dados da tabela freight_analysis
    freight_query = """
    SELECT * FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/freight_analysis');
    """
    freight_data = execute_query(freight_query)
    
    # Carregar dados da tabela regional_demand_supply_balance
    regional_query = """
    SELECT * FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/regional_demand_supply_balance');
    """
    regional_data = execute_query(regional_query)
    
    return freight_data, regional_data

try:
    freight_df, regional_df = load_data()
    
    # Filtros
    selected_categories = st.sidebar.multiselect(
        "Categoria de Produto",
        options=freight_df['product_category_name'].unique(),
        default=freight_df['product_category_name'].unique()[:3]
    )
except Exception as e:
    st.error(f"Erro ao carregar os dados: {e}")
    st.info("Verifique se as tabelas 'freight_analysis' e 'regional_demand_supply_balance' estão disponíveis no banco de dados.")
    st.stop()

selected_states = st.sidebar.multiselect(
    "Estado",
    options=regional_df['customer_state'].unique(),
    default=regional_df['customer_state'].unique()
)

# Filtragem dos dados
filtered_freight = freight_df[freight_df['product_category_name'].isin(selected_categories)]
filtered_regional = regional_df[regional_df['customer_state'].isin(selected_states)]

# 1. KPIs na primeira linha
col1, col2, col3, col4 = st.columns(4)

with col1:
    total_revenue = filtered_freight['total_revenue'].sum()
    st.metric("Receita Total", f"R$ {total_revenue:,.2f}")

with col2:
    total_orders = filtered_regional['total_orders'].sum()
    st.metric("Total de Pedidos", f"{total_orders:,}")

with col3:
    avg_freight = (filtered_freight['average_freight'] * filtered_freight['total_quantity_sold']).sum() / filtered_freight['total_quantity_sold'].sum()
    st.metric("Frete Médio", f"R$ {avg_freight:.2f}")

with col4:
    avg_demand_supply = filtered_regional['demand_supply_ratio'].mean()
    st.metric("Relação Média Demanda/Oferta", f"{avg_demand_supply:.2f}")

# 2. Análise de Frete e Categoria
st.subheader("Análise de Frete e Categoria")
col1, col2 = st.columns(2)

with col1:
    # Gráfico comparativo de frete médio vs preço médio
    df_for_plot = filtered_freight.copy()
    df_for_plot['price_freight_ratio'] = df_for_plot['average_price_per_item'] / df_for_plot['average_freight']
    
    fig = px.bar(
        df_for_plot,
        x='product_category_name',
        y=['average_price_per_item', 'average_freight'],
        barmode='group',
        labels={'value': 'Valor (R$)', 'product_category_name': 'Categoria', 'variable': 'Métrica'},
        title='Preço Médio vs Frete Médio por Categoria',
        color_discrete_map={'average_price_per_item': '#0f62fe', 'average_freight': '#78a9ff'}
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Gráfico de relação peso vs frete
    fig = px.scatter(
        filtered_freight,
        x='average_product_weight_per_category',
        y='average_freight',
        size='total_quantity_sold',
        color='product_category_name',
        hover_name='product_category_name',
        labels={
            'average_product_weight_per_category': 'Peso Médio (g)',
            'average_freight': 'Frete Médio (R$)',
            'product_category_name': 'Categoria'
        },
        title='Relação Peso vs Frete por Categoria'
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

# 3. Quantidade vendida x Receita Total
fig = px.bar(
    filtered_freight.sort_values('total_revenue', ascending=False),
    x='product_category_name',
    y='total_quantity_sold',
    labels={
        'product_category_name': 'Categoria',
        'total_quantity_sold': 'Quantidade Vendida',
        'total_revenue': 'Receita Total (R$)'
    },
    title='Quantidade Vendida por Categoria'
)
fig.add_trace(
    go.Scatter(
        x=filtered_freight.sort_values('total_revenue', ascending=False)['product_category_name'],
        y=filtered_freight.sort_values('total_revenue', ascending=False)['total_revenue'] / 10000,  # Dividindo para melhor visualização
        mode='lines+markers',
        name='Receita Total (R$ 10.000)',
        yaxis='y2'
    )
)
fig.update_layout(
    yaxis2=dict(
        title='Receita Total (R$ 10.000)',
        overlaying='y',
        side='right'
    ),
    height=500
)
st.plotly_chart(fig, use_container_width=True)

# 4. Análise Geográfica
st.subheader("Análise Geográfica")
col1, col2 = st.columns(2)

with col1:
    # Mapa de calor melhorado com escala de cores e legendas mais claras
    fig = px.density_map(
        filtered_regional,
        lat='avg_latitude',
        lon='avg_longitude',
        z='total_orders',
        radius=30,
        center=dict(lat=-15.77972, lon=-47.92972),  # Centralizado em Brasília
        zoom=3.5,
        labels={'total_orders': 'Total de Pedidos'},
        title='Concentração de Pedidos por Região',
        color_continuous_scale='Viridis'
    )
    fig.update_layout(
        height=500,
        coloraxis_colorbar=dict(
            title="Pedidos",
            thicknessmode="pixels", thickness=20,
            lenmode="pixels", len=300
        )
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Gráfico de barras da relação oferta/demanda por estado com melhorias visuais
    state_summary = filtered_regional.groupby('customer_state').agg({
        'demand_supply_ratio': 'mean',
        'total_orders': 'sum',
        'distinct_sellers': 'sum'
    }).reset_index()
    
    # Filtrar apenas os top 10 estados por demanda/oferta para visualização
    top_states = state_summary.sort_values('demand_supply_ratio', ascending=False).head(10)
    
    fig = px.bar(
        top_states,
        x='customer_state',
        y='demand_supply_ratio',
        color='demand_supply_ratio',
        text='demand_supply_ratio',
        labels={
            'customer_state': 'Estado',
            'demand_supply_ratio': 'Relação Demanda/Oferta'
        },
        title='Top 10 Estados por Relação Demanda/Oferta',
        color_continuous_scale=px.colors.diverging.RdYlGn_r,  # Escala de cores do vermelho ao verde
        height=500
    )
    
    # Formatação dos valores no gráfico
    fig.update_traces(texttemplate='%{text:.2f}', textposition='outside')
    
    # Adicionar linha de equilíbrio
    fig.add_trace(
        go.Scatter(
            x=top_states['customer_state'],
            y=[1] * len(top_states),
            mode='lines',
            name='Equilíbrio',
            line=dict(color='#ff7eb6', dash='dash', width=2)
        )
    )
    
    # Personalizar layout
    fig.update_layout(
        xaxis_title="Estados",
        yaxis_title="Relação Demanda/Oferta",
        legend_title="Legenda",
        xaxis_tickangle=-45,
        bargap=0.3  # Espaço entre as barras para melhor visualização
    )
    
    st.plotly_chart(fig, use_container_width=True)


# 5. Eficiência Logística Regional
st.subheader("Eficiência Logística Regional")

if not filtered_regional.empty:
    efficiency_df = filtered_regional.copy()

    efficiency_df.loc[:, 'distinct_sellers_float'] = efficiency_df['distinct_sellers'].astype(float).replace(0, np.nan)

    efficiency_df.loc[:, 'vendas_por_vendedor'] = efficiency_df['total_sales_value'] / efficiency_df['distinct_sellers_float']
    efficiency_df.loc[:, 'pedidos_por_vendedor'] = efficiency_df['total_orders'] / efficiency_df['distinct_sellers_float']
    efficiency_df.loc[:, 'clientes_por_vendedor'] = efficiency_df['total_customers'] / efficiency_df['distinct_sellers_float']

    # Agregação dos dados
    efficiency_table = efficiency_df.groupby('customer_state').agg({
        'total_orders': 'sum',
        'distinct_sellers': 'sum', # Soma de vendedores distintos pode ser interpretada como total de registros de vendedores por estado
        'total_customers': 'sum', # Similarmente, soma de clientes
        'total_sales_value': 'sum',
        'demand_supply_ratio': 'mean',
        'vendas_por_vendedor': 'mean',    # Média dos ratios calculados
        'pedidos_por_vendedor': 'mean',   # Média dos ratios calculados
        'clientes_por_vendedor': 'mean' # Média dos ratios calculados
    }).reset_index()

    # Ordenar e renomear colunas
    efficiency_table = efficiency_table.sort_values('demand_supply_ratio', ascending=False)
    efficiency_table = efficiency_table.rename(columns={
        'customer_state': 'Estado',
        'total_orders': 'Total Pedidos',
        'distinct_sellers': 'Vendedores (Soma Agrupada)', # Esclarecer o que 'distinct_sellers' representa após o 'sum'
        'total_customers': 'Clientes (Soma Agrupada)',  # Esclarecer
        'total_sales_value': 'Valor Total',
        'demand_supply_ratio': 'Demanda/Oferta',
        'vendas_por_vendedor': 'Vendas/Vendedor (Média)',
        'pedidos_por_vendedor': 'Pedidos/Vendedor (Média)',
        'clientes_por_vendedor': 'Clientes/Vendedor (Média)'
    })

    # tabela com estilização
    # na_rep para formatar valores NaN de forma explícita.
    # .set_properties(**{'color': 'black'}) para definir a cor da fonte como preta.
    st.dataframe(
        efficiency_table.style.format({
            'Valor Total': 'R$ {:,.2f}', # Adicionado separador de milhar
            'Demanda/Oferta': '{:.2f}',
            'Vendas/Vendedor (Média)': 'R$ {:,.2f}', # Adicionado separador de milhar
            'Pedidos/Vendedor (Média)': '{:.1f}',
            'Clientes/Vendedor (Média)': '{:.1f}',
            'Total Pedidos': '{:,}', # Formatação para inteiros com separador de milhar
            'Vendedores (Soma Agrupada)': '{:,}',
            'Clientes (Soma Agrupada)': '{:,}'
        }, na_rep='-') # Representa NaNs como '-' na tabela
        .set_properties(**{'color': 'black', # Define a cor do texto para preto
                           'border': '1px solid black'  # Define uma borda preta sólida de 1px para as células
                           }) 
        .apply(lambda x: ['background-color: #d1e7dd' if pd.notna(x['Demanda/Oferta']) and x['Demanda/Oferta'] > 1.2 
                            else ('background-color: #e0e0e0' if pd.notna(x['Demanda/Oferta']) and x['Demanda/Oferta'] >= 0.8 
                                  else ('background-color: #f8d7da' if pd.notna(x['Demanda/Oferta'])
                                        else 'background-color: #e9ecef')) # Cor para NaN em Demanda/Oferta
                            for _ in x], axis=1),
        use_container_width=True
    )
else:
    st.warning("Não há dados para exibir na tabela de Eficiência Logística Regional com os filtros selecionados.")

# 6. Análise de Oportunidades
st.subheader("Análise de Oportunidades")
col1, col2 = st.columns(2)

with col1:
    # Gráfico de radar para categorias por eficiência logística
    categories = filtered_freight.copy()
    categories['weight_to_freight_ratio'] = categories['average_product_weight_per_category'] / categories['average_freight']
    categories['price_to_freight_ratio'] = categories['average_price_per_item'] / categories['average_freight']
    
    # Normalizar para escala 0-1
    for col in ['weight_to_freight_ratio', 'price_to_freight_ratio', 'average_freight']:
        max_val = categories[col].max()
        min_val = categories[col].min()
        if max_val > min_val:
            categories[col + '_norm'] = (categories[col] - min_val) / (max_val - min_val)
        else:
            categories[col + '_norm'] = 0.5
    
    # Reverter a escala do frete (menor é melhor)
    categories['average_freight_norm'] = 1 - categories['average_freight_norm']
    
    # Gráfico de radar
    fig = go.Figure()
    
    for idx, row in categories.iterrows():
        fig.add_trace(go.Scatterpolar(
            r=[row['weight_to_freight_ratio_norm'], 
               row['price_to_freight_ratio_norm'], 
               row['average_freight_norm']],
            theta=['Peso/Frete', 'Preço/Frete', 'Frete Econômico'],
            fill='toself',
            name=row['product_category_name']
        ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 1]
            )),
        showlegend=True,
        title='Eficiência Logística por Categoria',
        height=500
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Gráfico de oportunidades por região
    fig = px.scatter(
        filtered_regional,
        x='distinct_sellers',
        y='total_customers',
        size='total_sales_value',
        color='customer_state',
        hover_name='customer_city',
        labels={
            'distinct_sellers': 'Quantidade de Vendedores',
            'total_customers': 'Quantidade de Clientes',
            'total_sales_value': 'Valor Total de Vendas',
            'customer_state': 'Estado'
        },
        title='Oportunidades por Região (Vendedores vs Clientes)'
    )
    
    # Adicionar linha diagonal (equilíbrio)
    max_val = max(filtered_regional['distinct_sellers'].max(), filtered_regional['total_customers'].max())
    fig.add_trace(
        go.Scatter(
            x=[0, max_val],
            y=[0, max_val],
            mode='lines',
            name='Equilíbrio',
            line=dict(color='black', dash='dash')
        )
    )
    
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)