# modules/query_interpreter.py

import json
import logging

class UserQueryAgent:
    """
    Agente encargado de interpretar consultas en lenguaje natural y convertirlas en una estructura
    de consulta estructurada (por ejemplo, un diccionario en formato JSON). La estructura resultante
    incluirá:
      - 'accion': La acción que se desea realizar (ej. contar, listar, promedio, etc.).
      - 'tabla': La tabla de la base de datos implicada.
      - 'filtros': Un objeto (diccionario) que representa las condiciones de la consulta en pares columna-valor.
    
    Este agente utiliza un modelo de lenguaje (LLM) para analizar la consulta en el contexto del esquema
    de la base de datos y del mapa semántico.
    """
    
    def __init__(self, llm_api_key=None, model="gpt-3.5-turbo", temperature=0.0):
        """
        :param llm_api_key: Clave API para el modelo de lenguaje (por ejemplo, OpenAI).
        :param model: Modelo de lenguaje a utilizar.
        :param temperature: Controla la aleatoriedad en la respuesta del modelo.
        """
        if llm_api_key:
            try:
                import openai
                openai.api_key = llm_api_key
            except ImportError:
                raise ImportError("El paquete openai no está instalado. Instálalo para usar el LLM.")
        self.model = model
        self.temperature = temperature
        self.logger = logging.getLogger(self.__class__.__name__)

    def interpretar_consulta(self, consulta, schema, semantic_map):
        """
        Interpreta una consulta en lenguaje natural y retorna una estructura de consulta (diccionario)
        con los campos 'accion', 'tabla' y 'filtros'.
        
        :param consulta: Consulta en lenguaje natural.
        :param schema: Esquema de la base de datos (diccionario obtenido, por ejemplo, con DBSchemaAgent).
        :param semantic_map: Mapa semántico para traducir nombres técnicos a nombres legibles.
        :return: Diccionario con la estructura de consulta.
        """
        prompt = self._crear_prompt(consulta, schema, semantic_map)
        respuesta_llm = self._obtener_respuesta_llm(prompt)
        
        try:
            estructura_consulta = json.loads(respuesta_llm)
        except json.JSONDecodeError as e:
            self.logger.error("Error al decodificar la respuesta del LLM: %s", e)
            estructura_consulta = {}
        
        return estructura_consulta

    def _crear_prompt(self, consulta, schema, semantic_map):
        """
        Crea el prompt para enviar al LLM, incluyendo el esquema de la base de datos, el mapa semántico
        y la consulta en lenguaje natural del usuario.
        
        :param consulta: Consulta en lenguaje natural.
        :param schema: Esquema de la base de datos en formato diccionario.
        :param semantic_map: Mapa semántico en formato diccionario.
        :return: Prompt completo en forma de cadena de texto.
        """
        prompt = (
            "Eres un asistente experto en bases de datos. Se te proporciona el esquema de la base de datos "
            "Analiza la consulta y, utilizando el esquema y el mapa semántico, identifica cuál es la tabla más relevante para responder a la pregunta, incluso si la consulta no menciona explícitamente el nombre de la tabla\n"
            "y un mapa semántico que convierte nombres técnicos a nombres legibles para humanos.\n\n"
            "Esquema de la base de datos (en formato JSON):\n"
            f"{json.dumps(schema, indent=2)}\n\n"
            "Mapa semántico (en formato JSON):\n"
            f"{json.dumps(semantic_map, indent=2)}\n\n"
            "Interpreta la siguiente consulta en lenguaje natural y genera una estructura de consulta en formato JSON. "
            "La estructura debe incluir los siguientes campos:\n"
            "- 'accion': La acción a realizar (por ejemplo, 'contar', 'listar', 'promedio').\n"
            "- 'tabla': El nombre de la tabla a consultar.\n"
            "- 'filtros': Un objeto con pares columna-valor que representen condiciones de la consulta.\n\n"
            "Ejemplo de salida:\n"
            '{\n'
            '  "accion": "contar",\n'
            '  "tabla": "carros",\n'
            '  "filtros": {\n'
            '    "color": "rojo"\n'
            '  }\n'
            '}\n\n'
            f"Consulta: {consulta}\n\n"
            "Estructura JSON:"
        )
        return prompt

    def _obtener_respuesta_llm(self, prompt):
        """
        Utiliza el modelo de lenguaje (por ejemplo, OpenAI GPT) para obtener una respuesta a partir del prompt.
        
        :param prompt: El prompt a enviar al modelo.
        :return: La respuesta generada por el LLM en formato de texto.
        """
        import openai
        try:
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=self.temperature,
                max_tokens=150
            )
            respuesta = response['choices'][0]['message']['content'].strip()
        except Exception as e:
            self.logger.error("Error al obtener respuesta del LLM: %s", e)
            respuesta = "{}"  # Retornamos un JSON vacío en caso de error.
        
        return respuesta
