# Projeto de Dados Olist

![Hero project image](./assets/static/olist_hero_banner.png)

## Descrição
Projeto de engenharia e análise de dados utilizando dados comerciais reais da plataforma brasileira de e-commerce Olist. A arquitetura consome uma única fonte de dados (em formato .csv) disponibilizada publicamente. Saiba mais sobre o conjunto de dados: [Kaggle - Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

## Sobre o Dataset
Conjunto de dados públicos de comércio eletrônico brasileiro da Olist. Este conjunto contém informações de 100 mil pedidos realizados entre 2016 e 2018 em diversos marketplaces no Brasil. Os dados permitem visualizar pedidos sob várias dimensões: status, preço, pagamento, desempenho do frete, localização do cliente, atributos do produto e avaliações escritas pelos clientes. Também está disponível um conjunto de dados de geolocalização que relaciona códigos postais brasileiros às coordenadas lat/lng.

Estes são dados comerciais reais que foram anonimizados. As referências às empresas e parceiros foram substituídas pelos nomes das grandes casas de Game of Thrones.

## Engenharia de Dados
A engenharia de dados deste projeto utilizou uma lista de ferramentas e métodos, incluindo:
- DuckDB
- Delta Lake
- Arquitetura Medalhão (Bronze, Silver e Gold)

### Arquitetura do Projeto
![Print Screen Schema](./assets/static/olist-data-engineering-schema.png)

## Esquema de Dados
Os dados estão divididos em vários conjuntos para melhor compreensão e organização. Consulte o esquema de dados a seguir ao trabalhar com ele:

![Schema dataset olist marketplace](./assets/static/schema_dataset_olist.png)

## Objetivo
O foco deste projeto é aplicar conceitos fundamentais de engenharia e análise de dados, permitindo entregar resultados por meio de dashboards, relatórios e utilizar Business Intelligence (BI).

## Dashboard BI
O dashboard deste projeto foi construído com as bibliotecas Python: Streamlit e Plotly. Ademais, este Data App possui um Agente de IA nativamente para auxiliar o usuário com tomadas de decisão e exploração dos dados do dashboard que vão além da visualização estática do Streamlit, permitindo compreender e responder perguntas complexas e estratégicas baseadas nos dados.

<video autoplay loop muted playsinline>
  <source src="./assets/video/dashboard_olist_data_project_demo.mp4" type="video/mp4">
  Your browser does not support the video tag.
</video>


### CoPilot Business Agent
O Agente de IA nativo do dashboard é chamado de Copilot Business Agent, ele funciona como ferramenta de exploração, análise e orientação baseado nos dados presentes no dashboard.

<video autoplay loop muted playsinline>
  <source src="./assets/video/copilot_business_agent_demo.mp4" type="video/mp4">
  Your browser does not support the video tag.
</video>


## Relatório BI
Aqui está um resumo do relatório de BI com insights, oportunidades de negócio e ações recomendadas que foram reveladas neste projeto:

### 🔍 Insights Relevantes Encontrados
- **Cancelamentos em baixa:** Apenas 2,38% de taxa, sinalizando boa satisfação dos clientes.
- **Entregas adiantadas:** Atraso médio negativo mostra eficiência logística.
- **Ticket médio saudável:** R$160,99 por venda, útil para ações de marketing.
- **Base de clientes sólida:** Potencial para segmentações mais estratégicas.
- **Variação nas vendas:** Algumas categorias com alto potencial de crescimento.

### 💡 Oportunidades de Negócio
- **Elevar satisfação:** Monitorar feedbacks negativos e reduzir ainda mais cancelamentos.
- **Aprimorar logística:** Replicar boas práticas de entregas rápidas em outras áreas.
- **Campanhas segmentadas:** Focar categorias com alta variabilidade e bom ticket.
- **Personalização:** Criar ofertas com base no perfil e comportamento dos clientes.
- **Treinamento de vendedores:** Suporte para quem está abaixo da média de performance.
- **Ajuste de preços e frete:** Testes para encontrar o ponto ótimo de receita.
- **Explorar novos mercados:** Expandir em regiões com demanda não atendida, como na cidade de Curitiba-PR, onde existe oportunidade de expansão (Índice de Demanda/Oferta de 1.6, sendo umas das maiores do país).

### ✅ Ações Recomendadas
- 🗣️ Pesquisas de satisfação para entender o cliente.
- 🎁 Programas de fidelização para aumentar a recompra.
- 📊 Monitoramento contínuo para ajustes estratégicos.
