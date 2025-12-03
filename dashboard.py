import streamlit as st # Biblioteca principal do Streamlit, usada para criar a interface web, gráficos e exibir dados.
import pandas as pd
import plotly.express as px # Biblioteca para criar gráficos interativos. 
import time
import os

# Configuração da Página
st.set_page_config(
    page_title="Dashboard Financeiro em Tempo Real",
    page_icon="💸",
    layout="wide"
)

# Título e Estilo CSS para os Cards
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    .big-font {
        font-size: 24px !important;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

st.title("📊 Dashboard de Transações Financeiras (Spark Streaming)")

# Função auxiliar para ler CSV sem quebrar se o arquivo estiver sendo escrito ou não existir
# Ela tenta ler o arquivo e retorna um DataFrame vazio em caso de falha
# Isso é útil para evitar erros quando o arquivo está sendo escrito pelo Spark
def ler_dados(arquivo):
    if os.path.exists(arquivo):
        try:
            return pd.read_csv(arquivo)
        except:
            return pd.DataFrame() # Retorna vazio se der erro de leitura (concorrência)
    return pd.DataFrame()

# Placeholders para o conteúdo (Isso permite atualizar a tela sem recarregar tudo)
kpi_container = st.container()
graficos_container = st.container()
instituicao_container = st.container()

# Loop de Atualização Automática
while True:
    # 1. Carregar Dados
    df_geral = ler_dados("dash_kpis_gerais.csv")
    df_mes = ler_dados("dash_analise_mes.csv")
    df_inst = ler_dados("dash_analise_inst.csv")
    
    # Verifica se temos dados para mostrar
    if df_geral.empty:
        with kpi_container:
            st.warning("Aguardando geração de dados pelo Spark... (Inicie o processador_spark.py)")
        time.sleep(2)
        continue

    # --- PROCESSAMENTO DOS DADOS PARA EXIBIÇÃO ---
    
    # Extraindo valores únicos do KPI Geral
    # KPI = Key Performance Indicator (Indicador Chave de Performance)
    # O arquivo dash_kpis_gerais.csv tem apenas 1 linha com os totais
    row = df_geral.iloc[0]
    
    # Extração dos valores
    # Obtém os valores dos KPIs do DataFrame, usando 0 como padrão se a chave não existir
    total_processado = row.get('total_processado', 0)
    maior_transacao = row.get('maior_transacao', 0)
    menor_transacao = row.get('menor_transacao', 0)
    media_transacao = row.get('media_geral', 0)
    qtd_total = row.get('qtd_total', 0)
    hora_pico = row.get('hora_pico', 0)
    tipo_comum = row.get('tipo_mais_comum', "N/A")

    # --- EXIBIÇÃO (LAYOUT) ---
    
    with kpi_container:
        # Linha 1 de Cards
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("💰 Valor Total Processado", f"R$ {total_processado:,.2f}")
        col2.metric("📈 Maior Transação", f"R$ {maior_transacao:,.2f}")
        col3.metric("📉 Menor Transação", f"R$ {menor_transacao:,.2f}")
        col4.metric("➗ Média das Transações", f"R$ {media_transacao:,.2f}")
        
        # Linha 2 de Cards
        col5, col6, col7 = st.columns(3)
        col5.metric("🔢 Qtd. Transações", f"{int(qtd_total)}")
        col6.metric("⏰ Hora de Maior Movimento", f"{int(hora_pico)}:00h")
        col7.metric("🏆 Tipo Mais Comum", f"{tipo_comum}")
        
        st.markdown("---")

    with graficos_container:
        st.subheader("📅 Análise Mensal")
        
        if not df_mes.empty:
            tab1, tab2, tab3 = st.tabs(["Volume (Qtd)", "Valor Total", "Ticket Médio"])
            
            with tab1:
                fig_qtd = px.bar(df_mes, x='mes_ano', y='qtd_mes', 
                                 title="Transações por Mês", text_auto=True, color='qtd_mes')
                st.plotly_chart(fig_qtd, use_container_width=True)
                
            with tab2:
                fig_total = px.line(df_mes, x='mes_ano', y='total_mes', 
                                    title="Valor Total por Mês (R$)", markers=True)
                fig_total.update_traces(line_color='#00CC96')
                st.plotly_chart(fig_total, use_container_width=True)
                
            with tab3:
                fig_media = px.area(df_mes, x='mes_ano', y='ticket_medio_mes', 
                                    title="Valor Médio por Mês (R$)", markers=True)
                st.plotly_chart(fig_media, use_container_width=True)
        else:
            st.info("Dados mensais ainda não disponíveis.")

    with instituicao_container:
        st.subheader("🏦 Análise por Instituição")
        col_inst1, col_inst2 = st.columns([1, 2])
        
        if not df_inst.empty:
            # Card / Tabela Lateral
            with col_inst1:
                st.markdown("### Ranking de Valores")
                st.dataframe(
                    df_inst.style.format({"soma_valor": "R$ {:,.2f}"}),
                    use_container_width=True,
                    hide_index=True
                )
            
            # Gráfico de Barras Deitado
            with col_inst2:
                fig_inst = px.bar(df_inst, x='soma_valor', y='instituicao', orientation='h',
                                  title="Valor Total por Instituição",
                                  text_auto='.2s', color='soma_valor')
                fig_inst.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_inst, use_container_width=True)
        else:
            st.info("Dados de instituições ainda não disponíveis.")

    # Intervalo de atualização (Simula o real-time)
    time.sleep(2)
    # O Streamlit reinicia o script automaticamente quando há interação, 
    # mas para loop infinito de dados externos usamos o rerun experimental ou 
    # apenas deixamos o loop rodar reescrevendo os containers (como feito acima).
    # O comando abaixo força o script a rodar de novo para pegar dados novos.
    st.rerun()