import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

# Função utilitária
def execute_query(query: str) -> pd.DataFrame:
    conn = duckdb.connect()
    result = conn.sql(query).fetchdf()
    conn.close()
    return result

# Layout e título
st.set_page_config(page_title="Dashboard de Produtos", page_icon="🛒", layout="wide")
st.title("🛒 Dashboard de Produtos")

# Função para agrupar categorias menos importantes como "Outros"
def group_small_categories(df, category_col, value_col, top_n=10):
    # Ordenar e pegar as top N categorias
    top_categories = df.groupby(category_col)[value_col].sum().nlargest(top_n).index.tolist()
    
    # Criar uma cópia do dataframe para não modificar o original
    df_grouped = df.copy()
    
    # Substituir categorias que não estão no top por "Outros"
    df_grouped.loc[~df_grouped[category_col].isin(top_categories), category_col] = "Outros"
    
    # Agregar os dados
    df_grouped = df_grouped.groupby(category_col).agg({
        col: 'sum' if df_grouped[col].dtype in [np.number] else 'first' 
        for col in df_grouped.columns if col != category_col
    }).reset_index()
    
    return df_grouped

# Carregando dados de produção
inventory_priority = execute_query("SELECT * FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_product_inventory_priority')")
freight_analysis = execute_query("SELECT * FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/freight_analysis')")
profitability_analysis = execute_query("SELECT * FROM delta_scan('/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/delta_lake/gold/gold_product_profitability_analysis')")

# ----- SIDEBAR COM FILTROS MELHORADOS -----
st.sidebar.header("Filtros")

# Container para manter o estado dos filtros
if 'first_load' not in st.session_state:
    st.session_state.first_load = True
    
# Filtro para número de categorias a exibir
top_n_categories = st.sidebar.slider("Mostrar Top N Categorias", min_value=5, max_value=20, value=10, step=1)

# Filtro de categorias (agora permite múltipla seleção)
all_categories = list(freight_analysis['product_category_name'].unique())
default_categories = sorted(freight_analysis.groupby('product_category_name')['total_revenue'].sum().nlargest(3).index.tolist())

# Opção para selecionar todas as categorias
select_all = st.sidebar.checkbox("Selecionar todas as categorias", value=False)

if select_all:
    selected_categories = all_categories
else:
    selected_categories = st.sidebar.multiselect(
        "Categorias de Produto (até 3 recomendado)", 
        options=all_categories,
        default=default_categories if default_categories else []
    )

# Se nenhuma categoria for selecionada, use as top 3 por receita
if not selected_categories:
    selected_categories = default_categories if default_categories else all_categories[:min(3, len(all_categories))]

# Para exibição detalhada, mostramos apenas a primeira categoria selecionada
primary_category = selected_categories[0] if selected_categories else ""

# Filtro de faixa de preço
min_price = float(profitability_analysis['avg_price'].min())
max_price = float(profitability_analysis['avg_price'].max())
price_range = st.sidebar.slider("Faixa de Preço", min_price, max_price, (min_price, max_price))

# Filtro de criticidade
criticality = st.sidebar.radio("Status de Criticidade", ["Todos", "Críticos", "Não Críticos"])

# Opção para exibição compacta ou detalhada dos gráficos
display_mode = st.sidebar.radio("Modo de Visualização", ["Compacto", "Detalhado"])

# Botão para atualizar o dashboard
update_dashboard = st.sidebar.button("Atualizar Dashboard")

# ----- PREPARAÇÃO DOS DADOS FILTRADOS -----
# Filtrando dados com base nas categorias selecionadas
filtered_freight = freight_analysis[freight_analysis['product_category_name'].isin(selected_categories)]
filtered_profitability = profitability_analysis[profitability_analysis['product_category'].isin(selected_categories)]
filtered_inventory = inventory_priority[inventory_priority['product_category'].isin(selected_categories)]

# Aplicando filtros adicionais para a análise detalhada
detailed_products = profitability_analysis[profitability_analysis['product_category'] == primary_category].copy()
detailed_products = detailed_products[
    (detailed_products['avg_price'] >= price_range[0]) & 
    (detailed_products['avg_price'] <= price_range[1])
]

if criticality == "Críticos":
    detailed_products = detailed_products[detailed_products['critical_product'] == True]
elif criticality == "Não Críticos":
    detailed_products = detailed_products[detailed_products['critical_product'] == False]

# ----- SEÇÃO 1: KPIs GERAIS -----
tab1, tab2, tab3 = st.tabs(["KPIs Gerais", "Análise de Produtos", "Análise de Frete"])

with tab1:
    st.header("KPIs Gerais")
    
    # Primeira linha de métricas
    col1, col2, col3, col4 = st.columns(4)
    
    # Cálculo dos KPIs - usando os dados filtrados
    if not filtered_freight.empty:
        total_revenue = filtered_freight['total_revenue'].sum()
        total_quantity = filtered_freight['total_quantity_sold'].sum()
        avg_ticket = total_revenue / total_quantity if total_quantity > 0 else 0
        avg_freight = filtered_freight['average_freight'].mean()
    else:
        total_revenue = 0
        total_quantity = 0
        avg_ticket = 0
        avg_freight = 0
    
    col1.metric("Receita Total", f"R$ {total_revenue:,.2f}")
    col2.metric("Total de Vendas", f"{total_quantity:,}")
    col3.metric("Ticket Médio", f"R$ {avg_ticket:.2f}")
    col4.metric("Frete Médio", f"R$ {avg_freight:.2f}")
    
    # Gráficos com categorias agrupadas
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Receita Total por Categoria")
        
        # Agrupando categorias menores em "Outros" para visualização mais limpa
        if display_mode == "Compacto":
            chart_data = group_small_categories(freight_analysis, 'product_category_name', 'total_revenue', top_n_categories)
        else:
            chart_data = filtered_freight
            
        # Ordenando por receita para melhor visualização
        chart_data = chart_data.sort_values('total_revenue', ascending=False)
        
        fig = px.bar(chart_data, x='product_category_name', y='total_revenue',
                     color='product_category_name', text_auto='.2s',
                     labels={'product_category_name': 'Categoria', 'total_revenue': 'Receita Total'})
        
        # Melhorias no layout
        fig.update_layout(
            height=400,
            xaxis_title="",
            legend_title="Categorias",
            margin=dict(l=20, r=20, t=30, b=20),
            xaxis={'categoryorder':'total descending'}
        )
        
        # Ocultar legenda se houver muitas categorias
        if len(chart_data) > 10:
            fig.update_layout(showlegend=False)
            
        # Rotacionar labels no eixo x para melhor legibilidade
        fig.update_xaxes(tickangle=45)
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("% Frete/Preço por Categoria")
        
        # Calculando proporção média de frete/preço por categoria
        freight_proportion = freight_analysis.copy()
        freight_proportion['freight_pct'] = freight_proportion['average_freight'] / freight_proportion['average_price_per_item'] * 100
        
        # Agrupando para visualização mais limpa
        if display_mode == "Compacto":
            chart_data = group_small_categories(freight_proportion, 'product_category_name', 'freight_pct', top_n_categories)
        else:
            chart_data = freight_proportion[freight_proportion['product_category_name'].isin(selected_categories)]
        
        fig = px.pie(chart_data, values='freight_pct', names='product_category_name',
                     hole=0.4, labels={'product_category_name': 'Categoria', 'freight_pct': '% Frete/Preço'})
        
        fig.update_layout(
            height=400,
            legend_title="Categorias",
            margin=dict(l=20, r=20, t=30, b=20)
        )
        
        # Se houver muitas categorias, limitamos o número de fatias exibidas
        if len(chart_data) > 10 and display_mode == "Compacto":
            fig.update_layout(showlegend=False)
            
        st.plotly_chart(fig, use_container_width=True)

    # Adicionando gráfico de tendência temporal (exemplo - ajustar conforme dados disponíveis)
    if len(selected_categories) <= 5:  # Mostrar apenas se poucas categorias estiverem selecionadas
        st.subheader("Comparação entre Categorias Selecionadas")
        
        comparison_metrics = ['total_revenue', 'average_freight', 'total_quantity_sold']
        selected_metric = st.selectbox("Métrica para Comparação", comparison_metrics)
        
        # Criando gráfico de comparação
        fig = px.bar(
            filtered_freight, 
            x='product_category_name', 
            y=selected_metric,
            color='product_category_name',
            labels={
                'product_category_name': 'Categoria', 
                'total_revenue': 'Receita Total',
                'average_freight': 'Frete Médio',
                'total_quantity_sold': 'Quantidade Vendida'
            }
        )
        
        fig.update_layout(
            height=400,
            xaxis_title="",
            legend_title="Categorias",
            margin=dict(l=20, r=20, t=30, b=20),
            xaxis={'categoryorder':'total descending'}
        )
        
        st.plotly_chart(fig, use_container_width=True)

# ----- SEÇÃO 2: ANÁLISE DE PRODUTOS -----
with tab2:
    st.header(f"Análise de Produtos - {primary_category}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Top Produtos por Prioridade de Vendas")
        # Filtrando apenas para a categoria principal
        category_inventory = inventory_priority[inventory_priority['product_category'] == primary_category]
        
        if not category_inventory.empty:
            st.dataframe(
                category_inventory[['product_id', 'product_category', 'total_sales', 'sales_count', 'avg_price_per_sale', 'sales_priority']]
                .sort_values('sales_priority')
                .head(5),
                hide_index=True
            )
        else:
            st.info(f"Não há dados de inventário para a categoria {primary_category}")
    
    with col2:
        st.subheader("Produtos Críticos")
        critical_products = detailed_products[detailed_products['critical_product'] == True]
        if not critical_products.empty:
            st.dataframe(
                critical_products[['product_id', 'product_category', 'avg_price', 'freight_pct_price']],
                hide_index=True
            )
        else:
            st.info(f"Não há produtos críticos na categoria {primary_category}")
    
    # Gráfico de dispersão preço x vendas melhorado
    st.subheader("Relação entre Preço e Quantidade Vendida")
    if not detailed_products.empty:
        fig = px.scatter(detailed_products, x="avg_price", y="total_sales", 
                        size="total_sales", color="critical_product",
                        hover_name="product_id", size_max=60,
                        labels={
                            'avg_price': 'Preço Médio', 
                            'total_sales': 'Total de Vendas', 
                            'critical_product': 'Produto Crítico'
                        },
                        color_discrete_map={True: 'red', False: 'blue'})
        
        fig.update_layout(
            height=500,
            legend_title="Produto Crítico",
            margin=dict(l=20, r=20, t=30, b=20),
            hovermode="closest"
        )
        
        # Adicionando linhas de referência para médias
        avg_price = detailed_products['avg_price'].mean()
        avg_sales = detailed_products['total_sales'].mean()
        
        fig.add_shape(
            type="line", line=dict(dash="dash", color="gray"),
            x0=avg_price, y0=0, x1=avg_price, y1=detailed_products['total_sales'].max()
        )
        
        fig.add_shape(
            type="line", line=dict(dash="dash", color="gray"),
            x0=0, y0=avg_sales, x1=detailed_products['avg_price'].max(), y1=avg_sales
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(f"Não há dados suficientes para exibir o gráfico para a categoria {primary_category}")
    
    # Distribuição de produtos críticos por categoria
    st.subheader("Distribuição de Produtos Críticos")
    
    # Preparando dados de todas as categorias para comparação
    critical_by_category = profitability_analysis.groupby('product_category')['critical_product'].sum().reset_index()
    critical_by_category.columns = ['product_category', 'critical_count']
    
    # Limitando para as top categorias ou categorias selecionadas
    if display_mode == "Compacto":
        chart_data = group_small_categories(critical_by_category, 'product_category', 'critical_count', top_n_categories)
    else:
        chart_data = critical_by_category[critical_by_category['product_category'].isin(selected_categories)]
    
    if not chart_data.empty:
        # Usando gráfico de barras horizontais para melhor visualização com muitas categorias
        fig = px.bar(
            chart_data.sort_values('critical_count', ascending=True), 
            y='product_category', 
            x='critical_count',
            orientation='h',
            color='product_category',
            labels={
                'product_category': 'Categoria', 
                'critical_count': 'Qtd. Produtos Críticos'
            }
        )
        
        fig.update_layout(
            height=max(400, len(chart_data) * 25),  # Ajuste dinâmico da altura
            showlegend=False,  # Esconder legenda pois as cores já estão no eixo Y
            margin=dict(l=20, r=20, t=30, b=20),
            yaxis={'categoryorder':'total ascending'}
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Não há produtos críticos para exibir")

# ----- SEÇÃO 3: ANÁLISE DE FRETE -----
with tab3:
    st.header(f"Análise de Frete e Peso")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Custo de Frete por Categoria")
        
        # Agrupando categorias para visualização mais limpa
        if display_mode == "Compacto":
            chart_data = group_small_categories(freight_analysis, 'product_category_name', 'average_freight', top_n_categories)
        else:
            chart_data = filtered_freight
            
        # Ordenando por frete médio para melhor visualização
        chart_data = chart_data.sort_values('average_freight', ascending=False)
        
        fig = px.bar(
            chart_data, 
            x='product_category_name', 
            y='average_freight',
            color='product_category_name', 
            text_auto=True,
            labels={
                'product_category_name': 'Categoria', 
                'average_freight': 'Frete Médio (R$)'
            }
        )
        
        fig.update_layout(
            height=400,
            xaxis_title="",
            showlegend=False,  # Ocultar legenda para melhor visualização
            margin=dict(l=20, r=20, t=30, b=20)
        )
        
        # Rotacionar labels no eixo x
        fig.update_xaxes(tickangle=45)
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Peso Médio por Categoria")
        
        # Agrupando categorias para visualização mais limpa
        if display_mode == "Compacto":
            chart_data = group_small_categories(
                freight_analysis, 
                'product_category_name', 
                'average_product_weight_per_category', 
                top_n_categories
            )
        else:
            chart_data = filtered_freight
            
        # Ordenando por peso médio para melhor visualização
        chart_data = chart_data.sort_values('average_product_weight_per_category', ascending=False)
        
        fig = px.bar(
            chart_data, 
            x='product_category_name', 
            y='average_product_weight_per_category',
            color='product_category_name', 
            text_auto=True,
            labels={
                'product_category_name': 'Categoria', 
                'average_product_weight_per_category': 'Peso Médio (g)'
            }
        )
        
        fig.update_layout(
            height=400,
            xaxis_title="",
            showlegend=False,  # Ocultar legenda para ocupar menos espaco
            margin=dict(l=20, r=20, t=30, b=20)
        )
        
        # Rotacionar labels no eixo x para melhor visualizar
        fig.update_xaxes(tickangle=45)
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Relação entre peso e custo de frete
    st.subheader("Relação entre Peso e Custo de Frete")
    
    # Usando apenas a categoria principal para detalhe
    category_products = profitability_analysis[profitability_analysis['product_category'] == primary_category]
    
    if not category_products.empty:
        fig = px.scatter(
            category_products, 
            x="avg_weight", 
            y="avg_freight", 
            size="total_sales", 
            color="critical_product",
            hover_name="product_id", 
            size_max=60,
            labels={
                'avg_weight': 'Peso Médio (g)', 
                'avg_freight': 'Frete Médio (R$)', 
                'critical_product': 'Produto Crítico'
            },
            color_discrete_map={True: 'red', False: 'blue'}
        )
        
        fig.update_layout(
            height=500,
            legend_title="Produto Crítico",
            margin=dict(l=20, r=20, t=30, b=20),
            hovermode="closest"
        )
        
        # Adicionando linha de tendência
        fig.update_layout(
            shapes=[
                dict(
                    type='line',
                    yref='y', xref='x',
                    x0=0, y0=0,
                    x1=category_products['avg_weight'].max(),
                    y1=category_products['avg_weight'].max() * 0.1,  # linha de tendência
                    line=dict(color='gray', width=2, dash='dash')
                )
            ]
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        # Mostra dados de todas as categorias selecionadas se não houver dados suficientes da categoria principal
        if not filtered_profitability.empty:
            fig = px.scatter(
                filtered_profitability, 
                x="avg_weight", 
                y="avg_freight", 
                size="total_sales", 
                color="product_category",
                hover_name="product_id", 
                size_max=60,
                labels={
                    'avg_weight': 'Peso Médio (g)', 
                    'avg_freight': 'Frete Médio (R$)', 
                    'product_category': 'Categoria'
                }
            )
            
            fig.update_layout(
                height=500,
                margin=dict(l=20, r=20, t=30, b=20),
                hovermode="closest"
            )
            
            st.plotly_chart(fig, use_container_width=True)
            st.info(f"Mostrando dados de todas as categorias selecionadas pois não há dados suficientes para a categoria {primary_category}")
        else:
            st.info(f"Não há dados suficientes para exibir o gráfico")
    
    # Mapa de calor: Correlação entre variáveis
    if st.checkbox("Mostrar Mapa de Correlação"):
        st.subheader("Mapa de Calor: Correlações")
        
        # Usando dados da categoria principal se houver dados suficientes
        if not category_products.empty and len(category_products) > 5:
            correlation_data = category_products[['avg_price', 'avg_freight', 'total_sales', 'avg_weight', 'freight_pct_price']]
            title = f"Correlações para categoria: {primary_category}"
        else:
            # se nao, usar todas as categorias selecionadas
            correlation_data = filtered_profitability[['avg_price', 'avg_freight', 'total_sales', 'avg_weight', 'freight_pct_price']]
            title = "Correlações globais (todas categorias selecionadas)"
            
        correlation_matrix = correlation_data.corr()
        
        fig = px.imshow(
            correlation_matrix, 
            text_auto=True, 
            aspect="auto",
            labels=dict(x="Variáveis", y="Variáveis", color="Correlação"),
            x=correlation_matrix.columns, 
            y=correlation_matrix.columns,
            color_continuous_scale='RdBu_r',  # Escala de cor
            zmin=-1, zmax=1  # Fixando escala para correlacao
        )
        
        fig.update_layout(
            height=500,
            title=title,
            margin=dict(l=20, r=20, t=50, b=20)
        )
        
        st.plotly_chart(fig, use_container_width=True)

# ----- SEÇÃO: VISÃO GERAL DE CATEGORIAS -----
tab4 = st.tabs(["Visão Geral de Categorias"])[0]

with tab4:
    st.header("Comparação de Categorias")
    
    # Seletor de métrica para visualização
    metrics = ["total_revenue", "total_quantity_sold", "average_freight", "average_price_per_item"]
    metric_labels = {
        "total_revenue": "Receita Total (R$)",
        "total_quantity_sold": "Quantidade Vendida",
        "average_freight": "Frete Médio (R$)",
        "average_price_per_item": "Preço Médio (R$)"
    }
    
    selected_metric = st.selectbox("Selecione a métrica para comparação", 
                                   options=metrics, 
                                   format_func=lambda x: metric_labels[x])
    
    # visualizacao treemap para comparacao de categorias
    st.subheader(f"Treemap: {metric_labels[selected_metric]} por Categoria")
    
    # agrupando categorias pequenas para melhor visualizacao
    if display_mode == "Compacto" or len(freight_analysis['product_category_name'].unique()) > top_n_categories:
        treemap_data = group_small_categories(freight_analysis, 'product_category_name', selected_metric, top_n_categories)
    else:
        treemap_data = freight_analysis
    
    fig = px.treemap(
        treemap_data, 
        path=['product_category_name'], 
        values=selected_metric,
        color=selected_metric,
        color_continuous_scale='Viridis',
        labels={
            'product_category_name': 'Categoria',
            selected_metric: metric_labels[selected_metric]
        }
    )
    
    fig.update_layout(
        height=600,
        margin=dict(l=20, r=20, t=30, b=20)
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # grafico comparativo: Top vs. Bottom categorias
    col1, col2 = st.columns(2)
    
    # top 5 categorias para a metrica selecionada
    top_5_categories = freight_analysis.groupby('product_category_name')[selected_metric].sum().nlargest(5)
    bottom_5_categories = freight_analysis.groupby('product_category_name')[selected_metric].sum().nsmallest(5)
    
    with col1:
        st.subheader(f"Top 5 Categorias - {metric_labels[selected_metric]}")
        
        fig = px.bar(
            top_5_categories.reset_index(), 
            y='product_category_name', 
            x=selected_metric,
            orientation='h',
            text_auto='.2s',
            labels={
                'product_category_name': 'Categoria',
                selected_metric: metric_labels[selected_metric]
            },
            color=selected_metric,
            color_continuous_scale='Viridis'
        )
        
        fig.update_layout(
            height=400,
            showlegend=False,
            margin=dict(l=20, r=20, t=30, b=20),
            yaxis={'categoryorder':'total ascending'}
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    with col2:
        st.subheader(f"Bottom 5 Categorias - {metric_labels[selected_metric]}")
        
        fig = px.bar(
            bottom_5_categories.reset_index(), 
            y='product_category_name', 
            x=selected_metric,
            orientation='h',
            text_auto='.2s',
            labels={
                'product_category_name': 'Categoria',
                selected_metric: metric_labels[selected_metric]
            },
            color=selected_metric,
            color_continuous_scale='Viridis'
        )
        
        fig.update_layout(
            height=400,
            showlegend=False,
            margin=dict(l=20, r=20, t=30, b=20),
            yaxis={'categoryorder':'total ascending'}
        )
        
        st.plotly_chart(fig, use_container_width=True)

st.divider()