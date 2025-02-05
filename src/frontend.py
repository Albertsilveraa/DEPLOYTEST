# frontend.py
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from app import process_query  # Importamos la función del backend
from data_analyzer import DataAnalysisAgent

# Configuración de la página
st.set_page_config(
    page_title="MySQL-Chat Bot",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos personalizados (opcional)
st.markdown("""
<style>
    .stApp {
        max-width: 1200px;
        margin: 0 auto;
    }
    .sql-code {
        background-color: #1e1e1e;
        color: #d4d4d4;
        padding: 10px;
        border-radius: 5px;
        font-family: 'Courier New', monospace;
    }
    .stButton>button {
        width: 100%;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar: Credenciales y configuración
with st.sidebar:
    st.header("📊 Configuración de la Base de Datos")
    openai_api_key = st.text_input("OpenAI API Key", type="password", key="openai_api_key")
    db_name = st.text_input("Nombre de la Base de Datos", key="db_name")
    db_user = st.text_input("Usuario", key="db_user")
    db_password = st.text_input("Contraseña", type="password", key="db_password")
    db_host = st.text_input("Host", value="localhost", key="db_host")
    db_port = st.text_input("Puerto", value="3306", key="db_port")
    if st.button("Actualizar Credenciales"):
        st.success("Credenciales actualizadas.")

# Crear un diccionario con la configuración de la base de datos
db_config = {
    "database": db_name,
    "user": db_user,
    "password": db_password,
    "host": db_host,
    "port": int(db_port) if db_port and db_port.isdigit() else 3306
}

# frontend.py


# Título y descripción principal
st.title("🤖 MySQL-Chat Bot")
st.markdown("### Tu asistente inteligente para consultas y análisis de base de datos")

# Historial de mensajes (para el chat)
if "messages" not in st.session_state:
    st.session_state.messages = []

# Mostrar historial de mensajes
st.markdown("## Historial de Conversación")
for message in st.session_state.messages:
    with st.container():
        if message["role"] == "user":
            st.markdown(f"<div class='chat-container'><div class='chat-header'>Usuario:</div>{message['content']}</div>", unsafe_allow_html=True)
        elif message["role"] == "assistant":
            content = message["content"]
            with st.container():
                st.markdown("<div class='chat-container'>", unsafe_allow_html=True)
                st.markdown("<div class='chat-header'>Asistente:</div>", unsafe_allow_html=True)
                if isinstance(content, dict):
                    if "message" in content:
                        st.markdown(content["message"])
                    if "sql_query" in content:
                        st.markdown("**Consulta SQL generada:**")
                        st.code(content["sql_query"], language="sql")
                    if "data" in content and content["data"]:
                        df = pd.DataFrame(content["data"], columns=content.get("columns", []))
                        st.dataframe(df)
                    if "analysis" in content:
                        st.markdown("### Análisis Estadístico")
                        agg_data = content["analysis"]
                        agg_df = pd.DataFrame(agg_data)
                        st.dataframe(agg_df)
                        # Generar gráfico comparativo usando DataAnalysisAgent
                        analysis_agent = DataAnalysisAgent(time_unit='ms')  # Notar que el agente usa 'ms' ya que la DB almacena milisegundos
                        fig = analysis_agent.plot_aggregated_data(
                            agg_df, 
                            time_column="timestamp", 
                            value_columns=["mean", "sum", "count"],
                            title="Análisis Comparativo",
                            ylabel="Valores"
                        )
                        st.pyplot(fig)
                else:
                    st.markdown(content)
                st.markdown("</div>", unsafe_allow_html=True)
        st.markdown("<div class='chat-divider'></div>", unsafe_allow_html=True)

# Entrada de consulta del usuario
st.markdown("## Ingresa tu consulta:")
user_input = st.text_input("Hazme una pregunta sobre la base de datos...", key="user_input")

if st.button("Enviar Consulta"):
    # Verificar credenciales y consulta
    required = [st.session_state.get("openai_api_key"), st.session_state.get("db_name"),
                st.session_state.get("db_user"), st.session_state.get("db_password"),
                st.session_state.get("db_host"), st.session_state.get("db_port")]
    if not all(required):
        st.error("Por favor, completa todas las credenciales en la barra lateral.")
    elif not user_input:
        st.error("Ingresa una consulta.")
    else:
        with st.spinner("Procesando consulta..."):
            db_config = {
                "database": st.session_state.get("db_name"),
                "user": st.session_state.get("db_user"),
                "password": st.session_state.get("db_password"),
                "host": st.session_state.get("db_host"),
                "port": int(st.session_state.get("db_port")) if st.session_state.get("db_port") and st.session_state.get("db_port").isdigit() else 3306
            }
            openai_api_key = st.session_state.get("openai_api_key")
            result = process_query(user_input, db_config, openai_api_key)
        
        # Construir el contenido de la respuesta
        assistant_content = {
            "sql_query": result["sql"],
            "data": result["resultados"]["data"] if result["resultados"] and "data" in result["resultados"] else [],
            "columns": result["resultados"]["columns"] if result["resultados"] and "columns" in result["resultados"] else [],
            "message": result["formatted_response"]
        }
        if result.get("analysis_result"):
            agg_data = result["analysis_result"]["agg_data"]
            assistant_content["analysis"] = agg_data
        
        st.session_state.messages.append({"role": "assistant", "content": assistant_content})
        if hasattr(st, "experimental_rerun"):
            st.experimental_rerun()
