# app.py

import mysql.connector
import pandas as pd
import datetime

from db_schema import DBSchemaAgent
from semantic_mapping import SemanticMappingAgent
from query_interpreter import UserQueryAgent
from sql_generator import SQLGenerationAgent
from query_executor import QueryExecutor
from response_formatter import ResponseFormatter
from data_analyzer import DataAnalysisAgent


def infer_table_from_query(query, semantic_map):
    """
    Intenta inferir la tabla a consultar a partir de la consulta en lenguaje natural
    y del mapa semántico.
    """
    query_lower = query.lower()
    # Buscar coincidencia completa en el nombre humanizado
    for table, info in semantic_map.items():
        human_table = info.get("human_name", table).lower()
        if human_table in query_lower:
            return table

    # Buscar coincidencias parciales (por palabra)
    for table, info in semantic_map.items():
        human_table = info.get("human_name", table).lower()
        for word in human_table.split():
            if word in query_lower:
                return table

    # Fallback: retornar la primera tabla si existe
    if semantic_map:
        return list(semantic_map.keys())[0]
    return None


def process_query(prompt, db_config, openai_api_key):
    """
    Orquesta la ejecución completa:
      - Si el usuario pregunta "qué puedes hacer", retorna una descripción de las funcionalidades.
      - Si el prompt menciona "placa", se ejecuta una consulta personalizada (con JOIN a detections) para buscar por número de placa.
      - De lo contrario, se sigue el flujo normal:
          • Extraer el esquema y generar el mapa semántico.
          • Interpretar la consulta en lenguaje natural.
          • Generar la consulta SQL.
          • Ejecutarla y, de ser necesario, realizar análisis adicional.
    """
    # Si el usuario pregunta qué puede hacer, se devuelve un mensaje de funcionalidades.
    if "que puedes hacer" in prompt.lower() or "qué puedes hacer" in prompt.lower():
        return {
            "estructura_consulta": {},
            "sql": "",
            "resultados": {},
            "formatted_response": ("Puedo ayudarte a contar, listar y comparar datos de la base de datos. "
                                   "Por ejemplo, puedo buscar objetos por número de placa, comparar la cantidad de "
                                   "carros de diferentes colores, analizar series temporales y mucho más. ¿En qué te puedo ayudar?"),
            "analysis_result": None
        }
    
    # Si el prompt menciona "placa", utilizar consulta personalizada
    if "placa" in prompt.lower():
        # Extraer el valor de la placa (se asume que se escribe de forma similar a: placa: ABQ874 o 'ABQ874')
        match = re.search(r"placa\s*(?:[:=]\s*|['\"])([A-Z0-9]+)", prompt, re.IGNORECASE)
        plate_value = match.group(1) if match else ""
        
        # Generar un query personalizado (ejemplo basado en tu consulta de referencia)
        sql_custom = f"""
SELECT 
    o.id AS object_id, 
    o.img_id,
    c.id AS cls, 
    c.category AS tipo, 
    m.camera_id AS camara,
    v.epoch AS fecha,
    m.location AS ubication,
    o.accuracy AS object_accuracy,
    d1.description AS plate, 
    d1.accuracy AS plate_accuracy,
    d2.description AS color,
    d2.accuracy AS color_accuracy
FROM object o
JOIN categories c ON c.id = o.category_id
JOIN videos v ON v.id = o.video_id
JOIN cameras m ON m.id = v.camera_id
JOIN detections d1 ON d1.object_id = o.id AND d1.attribute_id = 1 AND d1.status = 1 AND d1.description = '{plate_value}'
JOIN detections d2 ON d2.object_id = o.id AND d2.attribute_id = 2 AND d2.status = 1 AND d2.description = 'yellow'
WHERE (FROM_UNIXTIME(o.epoch / 1000 - 3600 * 5) BETWEEN '2025-02-02' AND '2025-02-05')
ORDER BY o.id DESC;
        """
        def get_connection():
            return mysql.connector.connect(
                host=db_config.get("host", "localhost"),
                user=db_config.get("user", ""),
                password=db_config.get("password", ""),
                database=db_config.get("database", ""),
                port=db_config.get("port", 3306)
            )
        query_executor = QueryExecutor(get_connection)
        result_custom = query_executor.ejecutar_sql(sql_custom)
        # Formatear la respuesta
        if result_custom and result_custom.get("data"):
            formatted_response = f"Resultados para la placa {plate_value}: se encontraron {result_custom['data'][0][0]} registros."
        else:
            formatted_response = f"No se encontraron resultados para la placa {plate_value}."
        return {
            "estructura_consulta": {"custom": True},
            "sql": sql_custom,
            "resultados": result_custom,
            "formatted_response": formatted_response,
            "analysis_result": None
        }
    
    # Flujo normal
    def get_connection():
        return mysql.connector.connect(
            host=db_config.get("host", "localhost"),
            user=db_config.get("user", ""),
            password=db_config.get("password", ""),
            database=db_config.get("database", ""),
            port=db_config.get("port", 3306)
        )
    
    db_name = db_config.get("database", "")
    # Extraer el esquema
    db_agent = DBSchemaAgent(get_connection, db_name, main_tables=None, include_sample_data=False)
    schema = db_agent.get_schema_dict()

    # Generar el mapa semántico
    semantic_agent = SemanticMappingAgent(custom_rules=None)
    semantic_map = semantic_agent.generate_map(schema)

    # Interpretar la consulta en lenguaje natural (usando OpenAI)
    user_query_agent = UserQueryAgent(llm_api_key=openai_api_key, model="gpt-3.5-turbo", temperature=0.0)
    estructura_consulta = user_query_agent.interpretar_consulta(prompt, schema, semantic_map)
    if not estructura_consulta.get("tabla"):
        inferred_table = infer_table_from_query(prompt, semantic_map)
        estructura_consulta["tabla"] = inferred_table

    # Generar la consulta SQL
    sql_generator = SQLGenerationAgent(limit=25)
    sql = sql_generator.generar_sql(estructura_consulta, schema)

    # Ejecutar la consulta SQL
    query_executor = QueryExecutor(get_connection)
    resultados = query_executor.ejecutar_sql(sql)

    # Formatear la respuesta en lenguaje natural
    response_formatter = ResponseFormatter()
    formatted_response = response_formatter.formatear_respuesta(resultados, estructura_consulta)

    # (Opcional) Análisis estadístico si la consulta incluye columnas de fechas
    analysis_result = None
    if resultados and resultados.get("columns") and resultados.get("data"):
        if "timestamp" in resultados["columns"]:
            df = pd.DataFrame(resultados["data"], columns=resultados["columns"])
            numeric_cols = [col for col in resultados["columns"] if col != "timestamp"]
            if numeric_cols:
                analysis_column = numeric_cols[0]
                analysis_agent = DataAnalysisAgent(time_unit='ms')
                df_converted = analysis_agent.convert_epoch_to_datetime(df.copy(), "timestamp")
                agg_df = analysis_agent.aggregate_by_time(df_converted, "timestamp", analysis_column, freq='D')
                analysis_result = {"agg_data": agg_df.to_dict(orient="list")}

    return {
        "estructura_consulta": estructura_consulta,
        "sql": sql,
        "resultados": resultados,
        "formatted_response": formatted_response,
        "analysis_result": analysis_result
    }


if __name__ == "__main__":
    # Ejemplo de uso local para pruebas
    db_config = {
        "database": "dev",
        "user": "root",
        "password": "rootpassword",
        "host": "10.23.63.53",
        "port": 3306
    }
    openai_api_key = ""
    prompt = input("Ingresa tu consulta en lenguaje natural: ")
    result = process_query(prompt, db_config, openai_api_key)
    
    print("Estructura de consulta interpretada:")
    print(result["estructura_consulta"])
    print("\nConsulta SQL generada:")
    print(result["sql"])
    print("\nRespuesta formateada:")
    print(result["formatted_response"])
    if result.get("analysis_result"):
        print("\nAnálisis estadístico:")
        print(result["analysis_result"])