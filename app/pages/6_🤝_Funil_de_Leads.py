import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import calendar

# Função utilitária
def execute_query(query: str) -> pd.DataFrame:
    conn = duckdb.connect()
    result = conn.sql(query).fetchdf()
    conn.close()
    return result

# Layout e título
st.set_page_config(page_title="Dashboard Leads", page_icon="🤝", layout="wide")
st.title("🤝 Dashboard de Análise de Leads Qualificados")

# Sidebar com filtros
st.sidebar.header("Filtros")

# Consulta para obter as datas mínima e máxima
date_range = execute_query("""
    SELECT 
        MIN(first_contact_date) AS min_date,
        MAX(first_contact_date) AS max_date
    FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_qualified_leads_priority')
""")

min_date = pd.to_datetime(date_range['min_date'].iloc[0])
max_date = pd.to_datetime(date_range['max_date'].iloc[0])

# Filtro de data
start_date = st.sidebar.date_input(
    "Data inicial",
    min_date,
    min_value=min_date,
    max_value=max_date
)
end_date = st.sidebar.date_input(
    "Data final",
    max_date,
    min_value=min_date,
    max_value=max_date
)

# Filtro de origem
origem_options = execute_query("SELECT DISTINCT came_from FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_qualified_leads_priority')").came_from.tolist()
origem_selected = st.sidebar.multiselect("Origem", origem_options, default=origem_options)

# Filtro de prioridade
prioridade_options = execute_query("SELECT DISTINCT priority FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_qualified_leads_priority')").priority.tolist()
prioridade_selected = st.sidebar.multiselect("Prioridade", prioridade_options, default=prioridade_options)

# Construir a condição WHERE para os filtros
where_conditions = []
if origem_selected:
    origem_str = ", ".join(f"'{o}'" for o in origem_selected)
    where_conditions.append(f"came_from IN ({origem_str})")
if prioridade_selected:
    prioridade_str = ", ".join(f"'{p}'" for p in prioridade_selected)
    where_conditions.append(f"priority IN ({prioridade_str})")

where_conditions.append(f"first_contact_date >= '{start_date}'")
where_conditions.append(f"first_contact_date <= '{end_date}'")

where_clause = " AND ".join(where_conditions)

# Aplicar os filtros nas consultas
filtered_data = execute_query(f"""
    SELECT * 
    FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_qualified_leads_priority')
    WHERE {where_clause}
    ORDER BY first_contact_date
""")

# KPIs na primeira linha
col1, col2, col3, col4 = st.columns(4)

# Total de leads
total_leads = execute_query(f"""
    SELECT COUNT(*) as total_leads
    FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_qualified_leads_priority')
    WHERE {where_clause}
""").iloc[0, 0]

# Leads por prioridade
leads_por_prioridade = execute_query(f"""
    SELECT priority, COUNT(*) as count
    FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_qualified_leads_priority')
    WHERE {where_clause}
    GROUP BY priority
""")

# Leads por origem
leads_por_origem = execute_query(f"""
    SELECT came_from, COUNT(*) as count
    FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_qualified_leads_priority')
    WHERE {where_clause}
    GROUP BY came_from
""")

# Média de leads por landing page
avg_leads_por_landing = execute_query(f"""
    SELECT AVG(total_leads) as avg_leads
    FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_qualified_leads_priority')
    WHERE {where_clause}
""").iloc[0, 0]

with col1:
    st.metric("Total de Leads", f"{total_leads}", border=True)
    
with col2:
    high_priority = leads_por_prioridade[leads_por_prioridade['priority'] == 'high']['count'].sum() if 'high' in leads_por_prioridade['priority'].values else 0
    total = leads_por_prioridade['count'].sum() if not leads_por_prioridade.empty else 1
    high_percent = (high_priority / total) * 100 if total > 0 else 0
    st.metric("Leads de Alta Prioridade", f"{high_priority} ({high_percent:.1f}%)", border=True)

with col3:
    organic_leads = leads_por_origem[leads_por_origem['came_from'] == 'organic_search']['count'].sum() if 'organic_search' in leads_por_origem['came_from'].values else 0
    st.metric("Leads Orgânicos", f"{organic_leads}", border=True)

with col4:
    st.metric("Média de Leads por Landing Page", f"{avg_leads_por_landing:.1f}",border=True)

st.divider()

# Segunda linha - Gráficos
col1, col2 = st.columns(2)

with col1:
    st.subheader("Distribuição de Leads por Origem")
    if not leads_por_origem.empty:
        fig = px.pie(
            leads_por_origem, 
            values='count', 
            names='came_from', 
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig.update_layout(legend_title="Origem")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem dados disponíveis para o período selecionado")

with col2:
    st.subheader("Distribuição de Leads por Prioridade")
    if not leads_por_prioridade.empty:
        fig = px.bar(
            leads_por_prioridade,
            x='priority',
            y='count',
            color='priority',
            color_discrete_sequence=px.colors.qualitative.Bold
        )
        fig.update_layout(
            xaxis_title="Prioridade",
            yaxis_title="Quantidade de Leads",
            legend_title="Prioridade"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem dados disponíveis para o período selecionado")

# Adicionar Funil de Conversão
st.subheader("Funil de Conversão por Prioridade")

# Obter dados ordenados por prioridade para o funil
if not leads_por_prioridade.empty:
    # Ordenar prioridades para o funil (high -> medium -> low)
    priority_order = {'high': 1, 'medium': 2, 'low': 3}
    ordered_data = leads_por_prioridade.copy()
    
    # Adicionar coluna para ordenação
    if 'priority' in ordered_data.columns:
        ordered_data['sort_order'] = ordered_data['priority'].map(priority_order)
        ordered_data = ordered_data.sort_values('sort_order')
        
        # Criar funil
        fig = go.Figure(go.Funnel(
            y=ordered_data['priority'],
            x=ordered_data['count'],
            textposition="inside",
            textinfo="value+percent initial",
            opacity=0.8,
            marker={"color": ["#1f77b4", "#ff7f0e", "#2ca02c"]},
            connector={"line": {"color": "royalblue", "width": 1}}
        ))
        
        fig.update_layout(
            title="Conversão de Leads por Prioridade",
            margin=dict(l=20, r=20, t=60, b=20)
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Dados insuficientes para criar o funil")
else:
    st.info("Sem dados disponíveis para o período selecionado")

# Terceira linha - Dados ao longo do tempo
st.subheader("Evolução de Leads ao Longo do Tempo")

# Preparar dados para o gráfico de evolução temporal
leads_por_dia = execute_query(f"""
    SELECT 
        CAST(first_contact_date AS DATE) AS date, 
        COUNT(*) as leads_count
    FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_qualified_leads_priority')
    WHERE {where_clause}
    GROUP BY date
    ORDER BY date
""")

if not leads_por_dia.empty:
    fig = px.line(
        leads_por_dia,
        x='date',
        y='leads_count',
        markers=True,
    )
    fig.update_layout(
        xaxis_title="Data",
        yaxis_title="Número de Leads",
        hovermode="x unified"
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Sem dados disponíveis para o período selecionado")

# Quarta linha - Tabela detalhada de leads com pesquisa
st.subheader("Tabela Detalhada de Leads")

# Adicionar campo de pesquisa
search_term = st.text_input("Pesquisar leads por ID")

# Consultar dados filtrados com pesquisa
if search_term:
    detailed_data = execute_query(f"""
        SELECT 
            mql_id, 
            first_contact_date, 
            came_from, 
            priority, 
            total_leads
        FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_qualified_leads_priority')
        WHERE {where_clause} AND mql_id LIKE '%{search_term}%'
        ORDER BY first_contact_date DESC
    """)
else:
    detailed_data = execute_query(f"""
        SELECT 
            mql_id, 
            first_contact_date, 
            came_from, 
            priority, 
            total_leads
        FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_qualified_leads_priority')
        WHERE {where_clause}
        ORDER BY first_contact_date DESC
    """)

# Exibir tabela com dados detalhados
st.dataframe(detailed_data, use_container_width=True)

# Adicionar um botão de download para os dados
@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

csv = convert_df_to_csv(detailed_data)
st.download_button(
    label="Baixar dados como CSV",
    data=csv,
    file_name='leads_data.csv',
    mime='text/csv',
)
