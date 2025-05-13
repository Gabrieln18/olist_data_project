import streamlit as st
import time
import os
import sys
from pathlib import Path

module_path = str(Path(__file__).parent.parent)
if module_path not in sys.path:
    sys.path.append(module_path)

from services.copilot_business_model_v1 import create_agent, AgentWrapper

# Configuração da página
st.set_page_config(
    page_title="CoPilot Business Agent",
    page_icon="💼",
    layout="wide",
)

# --- Funções Auxiliares ---
def initialize_agent():
    """
    Inicializa ou recupera a instância do agente do session_state.
    Recria o agente se as configurações relevantes (API key, temperatura) mudaram.
    Retorna a instância do agente ou None se a API key não estiver configurada.
    """
    openai_api_key = st.session_state.get("openai_api_input")
    temperature = st.session_state.get("temp_slider", 0.7) # Default se não estiver no slider ainda

    if not openai_api_key or not openai_api_key.strip():
        st.sidebar.warning("Por favor, insira sua OpenAI API Key e clique em 'Salvar Configurações'.")
        if "agent_instance" in st.session_state: # Limpa instância antiga se a key foi removida
            del st.session_state.agent_instance
        return None

    # verficar se o agente precisa ser recriado devido à mudança de configurações
    # Compara com as configurações usadas na última criação do agente 
    last_agent_config = st.session_state.get("last_agent_config", {})
    current_config = {"api_key": openai_api_key, "temp": temperature}

    if "agent_instance" not in st.session_state or last_agent_config != current_config:
        st.sidebar.info("Inicializando o agente com as novas configurações...")
        try:
            # create_agent deve retornar a instância do AgentWrapper
            agent_instance = create_agent(
                openai_api_key=openai_api_key,
                temperature=temperature
            )
            st.session_state.agent_instance = agent_instance
            st.session_state.last_agent_config = current_config # Armazena config usada
            st.sidebar.success("Agente pronto!")
            

            if not st.session_state.messages or \
               (len(st.session_state.messages) == 1 and "Erro" in st.session_state.messages[0]["content"]):
        
                welcome_message = "Olá! Espero que esteja tudo bem. Em que posso ser útil?"
                if not any(msg["content"] == welcome_message for msg in st.session_state.messages):
                    st.session_state.messages.append({"role": "assistant", "content": welcome_message})

        except ValueError as e:
            st.sidebar.error(f"Erro de configuração ao criar agente: {str(e)}")
            if "agent_instance" in st.session_state:
                del st.session_state.agent_instance # Remove instância falha
            return None
        except Exception as e:
            st.sidebar.error(f"Erro inesperado ao criar agente: {str(e)}")
            if "agent_instance" in st.session_state:
                del st.session_state.agent_instance
            return None
            
    return st.session_state.get("agent_instance")

# Função para inicializar o estado da sessão (chamada uma vez)
def initialize_session_state_once():
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    if "user_id" not in st.session_state:
        # Usar um ID único por sessão para a memória do agente
        st.session_state.user_id = f"streamlit_session_{int(time.time())}_{os.urandom(4).hex()}"

    # Inputs da sidebar precisam ser inicializados antes de serem usados
    # para que `initialize_agent` possa lê-los na primeira execução.
    if "openai_api_input" not in st.session_state:
        st.session_state.openai_api_input = os.getenv("OPENAI_API_KEY", "")
    
    if "temp_slider" not in st.session_state:
        st.session_state.temp_slider = 0.7

# Chamada de inicialização única
initialize_session_state_once()

# --- Configuração da Barra Lateral ---
with st.sidebar:
    st.title("⚙️ Configurações")
    
    st.header("API Key")
    st.text_input(
        "OpenAI API Key", 
        type="password",
        key="openai_api_input" # Este valor será usado por initialize_agent
    )
    
    st.header("Parâmetros do Modelo")
    st.slider(
        "Temperatura", 
        min_value=0.0, 
        max_value=1.0, 
        step=0.1,
        key="temp_slider" # Este valor será usado por initialize_agent
    )
    
    if st.button("Salvar e Reiniciar Agente"):
        # A simples alteração das chaves 'openai_api_input' ou 'temp_slider'
        # e a chamada de initialize_agent() na próxima interação/rerun
        # já cuidará da recriação. Este botão força um rerun e a verificação.
        st.success("Configurações aplicadas. O agente será reiniciado, se necessário.")
        # Forçar a limpeza da instância antiga para garantir a recriação com novas chaves.
        if "agent_instance" in st.session_state:
            del st.session_state.agent_instance
        st.rerun() 

    st.header("Gerenciar Conversa")
    if st.button("Limpar Conversa"):
        st.session_state.messages = []

        if "agent_instance" in st.session_state and st.session_state.agent_instance:
             welcome_message = "Olá! Espero que esteja tudo bem. Em que posso ser útil?"
             st.session_state.messages.append({"role": "assistant", "content": welcome_message})
        st.success("Conversa apagada com sucesso!")
        st.rerun()
    
    st.divider()
    st.markdown("### Sobre")
    st.markdown("""
    **CoPilot Business Agent**
    
    Um assistente impulsionado por IA para análise de dados e insights de negócios.
    """)

# --- Lógica Principal do Chat ---
st.title("💼 CoPilot Business Agent")
st.caption("Seu assistente de análise de dados e negócios. ``Verifique sua barra lateral para mais detalhes``.")

# Tenta inicializar/recuperar o agente
agent = initialize_agent()

# Exibir mensagens do histórico
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Processamento da entrada do usuário
if prompt := st.chat_input("Digite sua mensagem aqui..."):
    if not agent:
        st.warning("O agente não está configurado. Por favor, verifique suas configurações na barra lateral.")
        # Adiciona a mensagem do usuário mesmo assim, para que não se perca
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        # Adiciona uma mensagem de erro ao chat
        error_msg_chat = "⚠️ **Agente não configurado.** Verifique a API Key e clique em 'Salvar Configurações'."
        st.session_state.messages.append({"role": "assistant", "content": error_msg_chat})
        with st.chat_message("assistant"):
            st.markdown(error_msg_chat)
    else:
        # Adiciona a mensagem do usuário ao histórico
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Prepara para exibir a resposta do assistente
        with st.chat_message("assistant"):
            with st.spinner("Pensando..."):
                try:
                    # Obtem a resposta do agente usando a instância persistente
                    response = agent.get_response(prompt, user_id=st.session_state.user_id)
                    st.markdown(response)
                    st.session_state.messages.append({"role": "assistant", "content": response})
                except ValueError as e: # Erros específicos da lógica do agente
                    error_msg = f"⚠️ **Erro do Agente**: {str(e)}"
                    st.error(error_msg)
                    st.session_state.messages.append({"role": "assistant", "content": error_msg})
                except Exception as e: # Outros erros inesperados
                    error_msg = f"⚙️ **Ocorreu um erro inesperado**: {str(e)}"
                    st.error(error_msg)
                    st.session_state.messages.append({"role": "assistant", "content": error_msg})
