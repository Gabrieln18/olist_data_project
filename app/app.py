import streamlit as st
import streamlit as st

st.set_page_config(
    page_title="Análise de Dados - App Multipage",
    page_icon="📊",
)

st.title("📊 Aplicativo de Análise de Dados")
st.sidebar.success("Selecione uma página acima.")

st.markdown(
    """
    ### Bem-vindo ao Aplicativo de Análise de Dados!
    
    Este é um exemplo de aplicativo Streamlit com múltiplas páginas.
    
    ### O que você pode fazer aqui:
    - Visualizar e explorar dados
    - Analisar estatísticas
    - Criar visualizações personalizadas
    
    **👈 Selecione uma página no menu à esquerda** para começar!
    
    ### Como isso funciona?
    Este aplicativo usa o sistema nativo de múltiplas páginas do Streamlit.
    Cada página é um arquivo Python separado na pasta `pages/`.
    """
)

# Adicionando um pouco de informação sobre como a estrutura do projeto funciona
st.subheader("Estrutura do Projeto")
st.code("""
meu_app/
├── app.py           # Esta página principal
└── pages/
    ├── 1_📈_Visualizacao.py
    ├── 2_📊_Estatisticas.py 
    └── 3_⚙️_Configuracoes.py
""")