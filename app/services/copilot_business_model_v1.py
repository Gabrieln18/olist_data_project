from agno.agent import Agent
from agno.models.openai import OpenAIChat
from agno.tools.duckdb import DuckDbTools
from agno.memory.v2.memory import Memory
from agno.memory.v2.db.sqlite import SqliteMemoryDb
from agno.storage.sqlite import SqliteStorage
import os
from pathlib import Path

def create_agent(openai_api_key, temperature=0.7):
    """
    Cria e configura uma instância do agente CoPilot.
    
    Args:
        openai_api_key (str): Chave de API da OpenAI
        groq_api_key (str): Chave de API da Groq
        temperature (float): Valor de temperatura para geração de texto
        
    Returns:
        Agent: Instância configurada do agente
    """
    # Definição de caminhos relativos
    base_dir = Path(__file__).parent.parent
    db_file = base_dir / "tmp" / "agent.db"
    gold_database_path = "/media/gabriel/HD_Storage/Engenharia_de_Dados/olist_data_project/data/gold_database.db"
    
    # Cria diretório para o banco de dados se não existir
    os.makedirs(db_file.parent, exist_ok=True)
    
    # Template do prompt do sistema
    prompt_template = """
    #PERSONALIDADE
    Você é um CoPilot de análise de dados e negócios para empreendedores e profissionais.
    
    #ROTEIRO
    Ajude profissionais com análises e sugestões baseadas nos dados fornecidos ou disponíveis.
    
    #ÁREAS DE CONHECIMENTO
    Você possui conhecimento em marketing, vendas, finanças, operações, RH e estratégia empresarial.
    
    #CONDUTA
    - Analise dados de forma objetiva
    - Apresente sugestões fundamentadas em evidências
    - Ofereça explicações concisas e claras
    - Acompanhe o raciocínio do usuário
    - Questione quando necessário para esclarecer objetivos
    - Responda dúvidas com precisão
    
    #FORMATO DE SAÍDA
    - Use listas para pontos principais
    - Sugira visualizações quando apropriado
    - Destaque insights mais relevantes
    - Adapte o nível técnico ao usuário
    
    #LIMITAÇÕES
    Indique quando uma análise precisa de mais dados ou consulta especializada.
    
    #OBJETIVO
    Colaborar com análises de dados e entregar soluções de negócios de forma concisa e acionável.
    
    #COMPORTAMENTO
    Use linguagem convidativa, simples e amigável.
    
    #PANORAMA
    Você é um CoPilot que ajuda empreendedores e profissionais a tomarem decisões baseadas em dados para melhorar performance e resultados de negócios.
    
    #ADAPTABILIDADE
    Ajuste suas respostas ao nível de expertise do usuário e revise seu raciocínio continuamente para melhorar a qualidade das análises.
    
    #PRIMEIRO CONTATO
    Na primeira interação com o usuário, apresente-se brevemente e pergunte como pode ajudar com análise de dados ou negócios hoje.
    """
    
    # Configuração da memória
    memory = Memory(
        model=OpenAIChat(id="gpt-4o-mini", api_key=openai_api_key),
        db=SqliteMemoryDb(table_name="user_memories", db_file=str(db_file)),
    )

    # Configuração do storage
    storage = SqliteStorage(table_name="agent_sessions", db_file=str(db_file))
    
    # Configuração do modelo
    if not openai_api_key or not openai_api_key.strip():
        raise ValueError("É necessário fornecer uma OpenAI API key válida")
    
    model = OpenAIChat(
        id="gpt-4o-mini", 
        api_key=openai_api_key,
        temperature=temperature
    )
    
    # Criação do agente
    agent = Agent(
        model=model,
        system_message=prompt_template,
        memory=memory,
        storage=storage,
        add_history_to_messages=True,
        num_history_runs=10,
        enable_agentic_memory=True,
        instructions=["Ao iniciar uma conversa, seja breve, amigável e demonstre prontidão para ajudar o usuário."],
        markdown=True,
        tools=[DuckDbTools(db_path=gold_database_path)],
        show_tool_calls=False,
    )
    
    return AgentWrapper(agent)

class AgentWrapper:
    """
    Wrapper para o Agente para facilitar a integração com o Streamlit.
    """
    def __init__(self, agent):
        self.agent = agent
    
    def get_response(self, message, user_id="usuario_streamlit"):
        """
        Obtém uma resposta do agente.
        
        Args:
            message (str): Mensagem do usuário
            user_id (str): ID do usuário para rastrear memórias
            
        Returns:
            str: Resposta do agente
        """
        # O método correto para obter a resposta sem imprimir é run()
        response = self.agent.run(message, user_id=user_id)
        return response.content