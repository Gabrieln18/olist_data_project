import streamlit as st
import streamlit as st

st.set_page_config(
    page_title="Data App Olist Ecommerce",
    page_icon="📊",
)

st.title("📊 Aplicativo de Análise de Dados")
st.sidebar.success("Selecione uma página acima.")

st.markdown(
    """

    # Bem-vindo ao Aplicativo de Análise de Dados

    ## Introdução

    Este aplicativo Streamlit de múltiplas páginas disponibiliza análises completas dos dados da plataforma de e-commerce brasileira Olist. O projeto combina engenharia e análise de dados aplicadas a um conjunto de dados comerciais reais, disponibilizado publicamente em formato CSV. Para informações adicionais sobre o conjunto de dados, acesse: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

    O objetivo principal deste projeto é implementar conceitos fundamentais de engenharia e análise de dados, proporcionando resultados através de dashboards, relatórios e ferramentas de Business Intelligence (BI).

    ## Funcionalidades Disponíveis

    Este aplicativo permite:

    * Visualizar e explorar dados detalhados da plataforma
    * Analisar estatísticas comerciais relevantes
    * Criar visualizações personalizadas conforme suas necessidades

    **👈 Para começar, selecione uma página no menu à esquerda.**

    ## Estrutura Técnica

    O aplicativo utiliza o sistema nativo de múltiplas páginas do Streamlit. O dashboard integra um agente de IA nativo (CoPilot Business Agent) que possibilita interação com o usuário final, oferecendo orientação para tomadas de decisão e inteligência de negócios.

    **Observação importante:** Para utilizar o CoPilot Business Agent, é necessário possuir uma API KEY da OpenAI, requisito essencial para o funcionamento deste serviço nativo no Data App.

    ## Stack Tecnológica

    Este projeto implementa diversas tecnologias e fundamentos, abrangendo desde a engenharia até a análise de dados:

    * Arquitetura Medalhão (Medallion Architecture)
    * Delta Lake para armazenamento de dados em diferentes camadas
    * DuckDB para processamento dos pipelines de dados e consultas analíticas do dashboard
    * Agno Framework para criação e desenvolvimento do agente CoPilot Business Agent
    * Streamlit para construção do dashboard interativo
    * Plotly para visualizações de dados no Data App
    """
)

# Adicionando um pouco de informação sobre como a estrutura do projeto funciona
st.subheader("Estrutura do Projeto")
st.code("""
    pages
    ├── 1_💳_Pagamentos.py
    ├── 2_💲_Vendas.py
    ├── 3_👨‍💼_Vendedores.py
    ├── 4_🛒_Produtos.py
    ├── 5_👥_Clientes.py
    ├── 6_🤝_Funil_de_Leads.py
    ├── 7_📦_Logistica.py
    └── 8_💼_CoPilot_Business.py        
""")