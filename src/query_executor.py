# modules/query_executor.py

import logging

class QueryExecutor:
    """
    Agente encargado de ejecutar consultas SQL en la base de datos y retornar los resultados.
    
    Este agente utiliza una función 'get_connection' para obtener una conexión a la base de datos.
    Maneja errores y excepciones durante la ejecución y retorna los resultados en un formato estructurado.
    """
    
    def __init__(self, get_connection):
        """
        :param get_connection: Función que retorna una conexión a la base de datos.
        """
        self.get_connection = get_connection
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def ejecutar_sql(self, sql):
        """
        Ejecuta la consulta SQL proporcionada y retorna los resultados.
        
        :param sql: Consulta SQL a ejecutar (cadena de texto).
        :return: Un diccionario con las columnas y los datos obtenidos, o None en caso de error.
                 Ejemplo:
                 {
                    "columns": ["id", "nombre", "edad"],
                    "data": [
                        (1, "Juan", 30),
                        (2, "María", 25)
                    ]
                 }
        """
        conn = None
        cursor = None
        resultados = None
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            self.logger.info("Ejecutando SQL: %s", sql)
            cursor.execute(sql)
            # Obtener todos los registros
            data = cursor.fetchall()
            # Obtener nombres de columnas si están disponibles
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            resultados = {
                "columns": columns,
                "data": data
            }
        except Exception as e:
            self.logger.error("Error al ejecutar la consulta SQL: %s", e)
            resultados = None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        
        return resultados
