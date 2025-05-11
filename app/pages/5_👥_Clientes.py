import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime, timedelta

# Função utilitária
def execute_query(query: str) -> pd.DataFrame:
    conn = duckdb.connect()
    result = conn.sql(query).fetchdf()
    conn.close()
    return result

# Layout e título
st.set_page_config(page_title="Dashboard de Clientes", page_icon="👥", layout="wide")
st.title("👥 Dashboard de Clientes")

# Sidebar para filtros
st.sidebar.header("Filtros")

# Obter dados para filtros
states_query = """
SELECT DISTINCT customer_state 
FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/regional_demand_supply_balance')
ORDER BY customer_state
"""
states = execute_query(states_query)

cities_query = """
SELECT DISTINCT customer_city, customer_state
FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/regional_demand_supply_balance')
ORDER BY customer_state, customer_city
"""
cities = execute_query(cities_query)

# Filtros
selected_states = st.sidebar.multiselect(
    "Estado", 
    options=states["customer_state"].tolist(),
    default=states["customer_state"].tolist()[:5]  # Primeiros 5 estados como padrão
)

# Filtro dinâmico de cidades baseado nos estados selecionados
if selected_states:
    filtered_cities = cities[cities["customer_state"].isin(selected_states)]
    city_options = filtered_cities["customer_city"].tolist()
else:
    city_options = cities["customer_city"].tolist()

selected_cities = st.sidebar.multiselect(
    "Cidade",
    options=city_options,
    default=[]
)

# Filtro de período
today = datetime.now()
date_range = st.sidebar.date_input(
    "Período",
    value=(today - timedelta(days=180), today),
    max_value=today
)

# Construir condições de filtro para consultas SQL
state_filter = ""
if selected_states:
    state_list = ", ".join([f"'{s}'" for s in selected_states])
    state_filter = f"customer_state IN ({state_list})"

city_filter = ""
if selected_cities:
    city_list = ", ".join([f"'{c}'" for c in selected_cities])
    city_filter = f"customer_city IN ({city_list})"

# Combinando filtros
where_clause = ""
if state_filter or city_filter:
    where_clause = "WHERE "
    if state_filter:
        where_clause += state_filter
    if city_filter:
        where_clause += f" AND {city_filter}" if state_filter else city_filter

# Principal - KPIs no topo em 4 colunas
st.markdown("## 📊 Indicadores Principais")
kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

# KPI 1: Total de Clientes Gold
gold_customers_query = f"""
SELECT COUNT(*) as total_gold_customers,
       SUM(total_spent) as total_revenue,
       AVG(avg_ticket) as avg_ticket,
       MAX(last_order_date) as latest_order
FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_customers_segmented')
"""
gold_kpi = execute_query(gold_customers_query)

with kpi_col1:
    st.metric(
        label="Total Clientes Gold",
        value=f"{gold_kpi['total_gold_customers'].iloc[0]:,}".replace(",", ".")
    )

# KPI 2: Receita Total
with kpi_col2:
    st.metric(
        label="Receita Total (R$)",
        value=f"R$ {gold_kpi['total_revenue'].iloc[0]:,.2f}".replace(",", ".")
    )

# KPI 3: Ticket Médio
with kpi_col3:
    st.metric(
        label="Ticket Médio (R$)",
        value=f"R$ {gold_kpi['avg_ticket'].iloc[0]:,.2f}".replace(",", ".")
    )

# KPI 4: Data do Último Pedido
with kpi_col4:
    st.metric(
        label="Último Pedido",
        value=gold_kpi['latest_order'].iloc[0].strftime("%d/%m/%Y")
    )

st.divider()

# Primeira linha de gráficos
st.markdown("## 📈 Análise de Vendas por Região")
chart_col1, chart_col2 = st.columns(2)

# Gráfico 1: Mapa Interativo de Vendas por Estado (scatter_plot)
with chart_col1:
    sales_by_state_query = f"""
    SELECT customer_state, 
           AVG(CAST(avg_latitude AS FLOAT)) as latitude,
           AVG(CAST(avg_longitude AS FLOAT)) as longitude,
           ROUND(SUM(total_sales_value), 2) as total_sales,
           SUM(total_customers) as customers_count
    FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/regional_demand_supply_balance')
    {where_clause}
    GROUP BY customer_state
    """
    sales_by_state = execute_query(sales_by_state_query)
    
    # Scatter map com interatividade
    fig_sales_map = px.scatter_mapbox(
        sales_by_state,
        lat="latitude",
        lon="longitude",
        color="customer_state",
        size="total_sales",
        hover_name="customer_state",
        hover_data={"total_sales": True, "customers_count": True, "latitude": False, "longitude": False},
        zoom=3.5,
        height=400,
        title="Vendas Totais por Estado (Interativo)",
        mapbox_style="carto-positron",
        size_max=30,
        labels={
            "total_sales": "Vendas Totais (R$)",
            "customers_count": "Número de Clientes",
            "customer_state": "Estado"
        }
    )
    fig_sales_map.update_layout(
        margin={"r": 0, "t": 40, "l": 0, "b": 0},
        mapbox=dict(
            bearing=0,
            pitch=0
        )
    )
    st.plotly_chart(fig_sales_map, use_container_width=True)

# Gráfico 2: Top 10 Cidades por Receita
with chart_col2:
    top_cities_query = f"""
    SELECT customer_city, customer_state, SUM(total_sales_value) as total_sales
    FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/regional_demand_supply_balance')
    {where_clause}
    GROUP BY customer_city, customer_state
    ORDER BY total_sales DESC
    LIMIT 10
    """
    top_cities = execute_query(top_cities_query)
    
    # Adicionando interatividade com cores por estado
    fig_top_cities = px.bar(
        top_cities,
        x="total_sales",
        y="customer_city",
        orientation="h",
        title="Top 10 Cidades por Receita",
        labels={"total_sales": "Receita Total (R$)", "customer_city": "Cidade"},
        text=top_cities["total_sales"].apply(lambda x: f"R$ {x:,.2f}".replace(",", ".")),
        color="customer_state",
        hover_data=["customer_state"]
    )
    fig_top_cities.update_layout(
        height=400,
        yaxis={"categoryorder": "total ascending"}
    )
    st.plotly_chart(fig_top_cities, use_container_width=True)

# Segunda linha de gráficos
st.markdown("## 🔍 Análise de Demanda e Oferta")
chart_col3, chart_col4 = st.columns(2)

# Gráfico 3: Relação Demanda/Oferta por Estado
with chart_col3:
    demand_supply_query = f"""
    SELECT customer_state, 
           ROUND(AVG(demand_supply_ratio), 2) as avg_ratio,
           SUM(total_orders) as total_orders,
           SUM(distinct_sellers) as total_sellers
    FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/regional_demand_supply_balance')
    {where_clause}
    GROUP BY customer_state
    ORDER BY avg_ratio DESC
    """
    demand_supply = execute_query(demand_supply_query)
    
    fig_demand = px.scatter(
        demand_supply,
        x="total_orders",
        y="total_sellers",
        size="avg_ratio",
        color="customer_state",
        hover_name="customer_state",
        size_max=50,
        title="Relação Demanda (Pedidos) vs Oferta (Vendedores)",
        labels={
            "total_orders": "Total de Pedidos",
            "total_sellers": "Total de Vendedores",
            "avg_ratio": "Razão Média Demanda/Oferta"
        }
    )
    # Adicionar interatividade
    fig_demand.update_layout(
        height=400,
        xaxis=dict(
            rangeslider=dict(visible=True),
            type="linear"
        )
    )
    st.plotly_chart(fig_demand, use_container_width=True)

# Gráfico 4: Distribuição de Ticket Médio dos Clientes Gold
with chart_col4:
    ticket_distribution_query = """
    SELECT 
        CASE 
            WHEN avg_ticket < 100 THEN '< R$100'
            WHEN avg_ticket BETWEEN 100 AND 200 THEN 'R$100-200'
            WHEN avg_ticket BETWEEN 200 AND 500 THEN 'R$200-500'
            WHEN avg_ticket BETWEEN 500 AND 1000 THEN 'R$500-1000'
            ELSE '> R$1000'
        END as ticket_range,
        COUNT(*) as customer_count
    FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_customers_segmented')
    GROUP BY ticket_range
    ORDER BY 
        CASE 
            WHEN ticket_range = '< R$100' THEN 1
            WHEN ticket_range = 'R$100-200' THEN 2
            WHEN ticket_range = 'R$200-500' THEN 3
            WHEN ticket_range = 'R$500-1000' THEN 4
            ELSE 5
        END
    """
    ticket_distribution = execute_query(ticket_distribution_query)
    
    # Gráfico interativo de pizza
    fig_ticket = px.pie(
        ticket_distribution,
        names="ticket_range",
        values="customer_count",
        title="Distribuição de Clientes Gold por Ticket Médio",
        hole=0.4,
        color_discrete_sequence=px.colors.sequential.Viridis
    )
    fig_ticket.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hoverinfo='label+percent+value'
    )
    fig_ticket.update_layout(height=400)
    st.plotly_chart(fig_ticket, use_container_width=True)

# Terceira linha - Métricas de Clientes Gold e Mapas
st.markdown("## 💎 Análise de Clientes Gold")
metrics_col1, metrics_col2 = st.columns(2)

# Gráfico 5: Evolução de Frequência de Compra
with metrics_col1:
    frequency_query = """
    WITH customer_frequency AS (
        SELECT 
            customer_id,
            DATEDIFF('day', first_order_date, last_order_date) / NULLIF(total_orders - 1, 0) as days_between_orders
        FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_customers_segmented')
        WHERE total_orders > 1
    )
    SELECT 
        CASE 
            WHEN days_between_orders <= 30 THEN 'Mensal'
            WHEN days_between_orders <= 90 THEN 'Trimestral'
            WHEN days_between_orders <= 180 THEN 'Semestral'
            ELSE 'Anual ou mais'
        END as frequency_group,
        COUNT(*) as customer_count
    FROM customer_frequency
    GROUP BY frequency_group
    ORDER BY 
        CASE 
            WHEN frequency_group = 'Mensal' THEN 1
            WHEN frequency_group = 'Trimestral' THEN 2
            WHEN frequency_group = 'Semestral' THEN 3
            ELSE 4
        END
    """
    frequency_data = execute_query(frequency_query)
    
    # Gráfico interativo de barras
    fig_frequency = px.bar(
        frequency_data, 
        x="frequency_group", 
        y="customer_count",
        title="Frequência de Compra - Clientes Gold",
        labels={"frequency_group": "Frequência", "customer_count": "Número de Clientes"},
        color="frequency_group",
        text="customer_count"
    )
    fig_frequency.update_layout(
        xaxis={'categoryorder': 'array', 'categoryarray': ['Mensal', 'Trimestral', 'Semestral', 'Anual ou mais']}
    )
    st.plotly_chart(fig_frequency, use_container_width=True)

# Gráfico 7: Distribuição Geográfica dos Clientes Gold - COM ZOOM DINÂMICO
with metrics_col2:
    geo_gold_query = f"""
    SELECT 
        g.customer_id,
        g.avg_latitude,
        g.avg_longitude,
        g.total_spent,
        g.avg_ticket,
        r.customer_city,
        r.customer_state
    FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_customers_segmented') g
    JOIN delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/regional_demand_supply_balance') r ON g.customer_cep = r.customer_cep
    WHERE g.avg_latitude IS NOT NULL AND g.avg_longitude IS NOT NULL
    {'' if not where_clause else where_clause.replace('WHERE', 'AND')}
    LIMIT 500
    """
    geo_gold_data = execute_query(geo_gold_query)
    
    # Mapa interativo com zoom avançado
    fig_geo = px.scatter_mapbox(
        geo_gold_data,
        lat="avg_latitude",
        lon="avg_longitude",
        color="avg_ticket",
        size="total_spent",
        hover_name="customer_id",
        hover_data={
            "customer_city": True, 
            "customer_state": True, 
            "total_spent": True, 
            "avg_ticket": True,
            "avg_latitude": False,
            "avg_longitude": False
        },
        color_continuous_scale="Viridis",
        size_max=15,
        zoom=4,
        mapbox_style="carto-positron",
        title="Distribuição Geográfica dos Clientes Gold (Com Zoom Dinâmico)"
    )
    
    # Habilitando zoom dinâmico e melhorando interatividade
    fig_geo.update_layout(
        margin={"r": 0, "t": 40, "l": 0, "b": 0},
        mapbox=dict(
            bearing=0,
            pitch=0,
            zoom=4,
            center=dict(
                lat=geo_gold_data["avg_latitude"].mean(),
                lon=geo_gold_data["avg_longitude"].mean()
            )
        ),
        # Adicionando controles de zoom
        updatemenus=[
            dict(
                type = "buttons",
                direction = "left",
                buttons=[
                    dict(
                        args=[{"mapbox.zoom": 3}],
                        label="Zoom Out",
                        method="relayout"
                    ),
                    dict(
                        args=[{"mapbox.zoom": 7}],
                        label="Zoom In",
                        method="relayout"
                    )
                ],
                pad={"r": 10, "t": 10},
                showactive=False,
                x=0.1,
                xanchor="left",
                y=1.1,
                yanchor="top"
            )
        ]
    )
    st.plotly_chart(fig_geo, use_container_width=True)

# Tabela com principais informações regionais - ADICIONANDO RECURSOS INTERATIVOS
st.markdown("## 📋 Detalhamento Regional")

regional_summary_query = f"""
SELECT 
    customer_state as Estado,
    COUNT(DISTINCT customer_city) as Cidades,
    SUM(total_customers) as TotalClientes,
    SUM(total_orders) as TotalPedidos,
    ROUND(SUM(total_sales_value), 2) as ReceitaTotal,
    ROUND(SUM(total_orders) / SUM(total_customers), 2) as PedidosPorCliente,
    ROUND(SUM(total_sales_value) / SUM(total_orders), 2) as TicketMedio,
    ROUND(AVG(demand_supply_ratio), 2) as IndiceDemandaOferta
FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/regional_demand_supply_balance')
{where_clause}
GROUP BY customer_state
ORDER BY ReceitaTotal DESC
"""
regional_summary = execute_query(regional_summary_query)

formatted_values = [
    regional_summary[col] if col not in ["ReceitaTotal", "PedidosPorCliente", "TicketMedio", "IndiceDemandaOferta"]
    else [
        f"R$ {x:,.2f}".replace(",", ".") if col in ["ReceitaTotal", "TicketMedio"] else f"{x:.2f}"
        for x in regional_summary[col]
    ]
    for col in regional_summary.columns
]


# Criando uma versão interativa do dataframe usando plotly
fig_table = go.Figure(data=[go.Table(
    header=dict(
        values=list(regional_summary.columns),
        fill_color='#075E54',
        align='left',
        font=dict(color='white', size=12)
    ),
    cells=dict(
    values=formatted_values,
    fill_color='#f3f3f3',
    align='left',
    font=dict(color='#333333', size=11)
)

)])

fig_table.update_layout(
    title="Tabela Interativa - Detalhamento Regional",
    height=400
)

st.plotly_chart(fig_table, use_container_width=True)

# Adicionando gráficos interativos ao lado do dataframe
chart_col5, chart_col6 = st.columns(2)

# Gráfico de barras para comparação regional
with chart_col5:
    fig_regional_bar = px.bar(
        regional_summary,
        x="Estado",
        y="ReceitaTotal",
        color="IndiceDemandaOferta",
        hover_data=["TotalClientes", "TotalPedidos", "PedidosPorCliente"],
        title="Comparativo de Receita Total por Estado",
        color_continuous_scale="Viridis"
    )
    fig_regional_bar.update_layout(height=350)
    st.plotly_chart(fig_regional_bar, use_container_width=True)

# Gráfico de dispersão para análise de correlação
with chart_col6:
    fig_scatter = px.scatter(
        regional_summary,
        x="TotalClientes",
        y="TicketMedio",
        size="ReceitaTotal",
        color="Estado",
        hover_name="Estado",
        title="Relação entre Clientes e Ticket Médio",
        labels={"TotalClientes": "Total de Clientes", "TicketMedio": "Ticket Médio (R$)"}
    )
    fig_scatter.update_layout(height=350)
    st.plotly_chart(fig_scatter, use_container_width=True)

# Mapa de Calor de Cidades com alta demanda e baixa oferta
st.markdown("## 🎯 Oportunidades Regionais")
opportunity_query = f"""
SELECT 
    customer_city as Cidade,
    customer_state as Estado,
    total_customers as Clientes,
    total_orders as Pedidos,
    distinct_sellers as Vendedores,
    ROUND(demand_supply_ratio, 2) as RatioDemandaOferta,
    ROUND(total_sales_value, 2) as ReceitaTotal,
    avg_latitude as Latitude,
    avg_longitude as Longitude
FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/regional_demand_supply_balance')
WHERE demand_supply_ratio > 1.5
  AND total_customers > 10
  {'' if not where_clause else 'AND ' + where_clause.replace('WHERE', '')}
ORDER BY demand_supply_ratio DESC, total_orders DESC
LIMIT 20
"""
opportunity_data = execute_query(opportunity_query)

# Criando um mapa interativo de oportunidades
fig_opportunity_map = px.scatter_mapbox(
    opportunity_data,
    lat="Latitude",
    lon="Longitude",
    size="RatioDemandaOferta",
    color="ReceitaTotal",
    hover_name="Cidade",
    hover_data={
        "Estado": True,
        "Clientes": True,
        "Pedidos": True, 
        "Vendedores": True,
        "RatioDemandaOferta": True,
        "ReceitaTotal": True,
        "Latitude": False,
        "Longitude": False
    },
    color_continuous_scale="Viridis",
    size_max=20,
    zoom=3.5,
    center={"lat": -10.0000, "lon": -55.0000},  # Corrigido: 'lng' para 'lon'
    mapbox_style="carto-positron",
    title="Mapa de Oportunidades Regionais (Cidades com Alta Demanda e Baixa Oferta)"
)

fig_opportunity_map.update_layout(
    height=500,
    margin={"r": 0, "t": 40, "l": 0, "b": 0}
)

st.plotly_chart(fig_opportunity_map, use_container_width=True)

# Formatação para exibir em tabela
opportunity_data["Receita Total"] = opportunity_data["ReceitaTotal"].apply(
    lambda x: f"R$ {x:,.2f}".replace(",", ".")
)

st.markdown("### Cidades com Alta Demanda e Baixa Oferta")
st.markdown("Locais com potencial para expansão de vendedores")

# Tabela interativa de oportunidades
fig_opportunity_table = go.Figure(data=[go.Table(
    header=dict(
        values=["Cidade", "Estado", "Clientes", "Pedidos", "Vendedores", "Ratio Demanda/Oferta", "Receita Total"],
        fill_color='#075E54',
        align='left',
        font=dict(color='white', size=12)
    ),
    cells=dict(
        values=[
            opportunity_data["Cidade"],
            opportunity_data["Estado"],
            opportunity_data["Clientes"],
            opportunity_data["Pedidos"],
            opportunity_data["Vendedores"],
            opportunity_data["RatioDemandaOferta"].apply(lambda x: f"{x:.2f}"),
            opportunity_data["Receita Total"]
        ],
        fill_color=[
            ['#f3f3f3', '#ffffff'] * len(opportunity_data)
        ],
        align='left',
        font=dict(color='#333333', size=11)
    )
)])

fig_opportunity_table.update_layout(
    height=400,
    margin=dict(l=0, r=0, b=0, t=0)
)

st.plotly_chart(fig_opportunity_table, use_container_width=True)

# Adicionar botões de download
col_download1, col_download2 = st.columns(2)
with col_download1:
    csv = regional_summary.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download Detalhamento Regional (CSV)",
        data=csv,
        file_name="detalhamento_regional.csv",
        mime="text/csv"
    )

with col_download2:
    csv_opportunity = opportunity_data.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download Oportunidades Regionais (CSV)",
        data=csv_opportunity,
        file_name="oportunidades_regionais.csv",
        mime="text/csv"
    )

# Nota de rodapé
st.markdown("---")
st.caption(f"Dashboard atualizado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

# Adicionar controles interativos para o usuário
with st.expander("Configurações do Dashboard"):
    st.markdown("### Opções de Visualização")
    col_opt1, col_opt2, col_opt3 = st.columns(3)
    
    with col_opt1:
        map_style = st.selectbox(
            "Estilo de Mapa",
            ["carto-positron", "open-street-map", "carto-darkmatter", "stamen-terrain"],
            index=0
        )
        
    with col_opt2:
        color_scale = st.selectbox(
            "Esquema de Cores",
            ["Viridis", "Plasma", "Inferno", "Magma", "Cividis", "Rainbow"],
            index=0
        )
        
    with col_opt3:
        show_animations = st.checkbox("Mostrar Animações", value=True)
    
    st.markdown("*Atualize a página para aplicar as configurações*")