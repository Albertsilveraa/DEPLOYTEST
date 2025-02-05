# modules/response_formatter.py

class ResponseFormatter:
    """
    Agente encargado de formatear los resultados obtenidos de la consulta SQL en una respuesta
    legible en lenguaje natural.
    
    Dependiendo del tipo de consulta, se puede:
      - Mostrar un mensaje simple para resultados agregados (ej. contar, promedio).
      - Formatear una tabla para resultados de listas.
    """
    
    def __init__(self):
        pass

    def formatear_respuesta(self, resultados, estructura_consulta=None):
        """
        Recibe los resultados obtenidos (un diccionario con claves "columns" y "data") y 
        genera una respuesta legible en lenguaje natural.
        
        :param resultados: Diccionario con el resultado de la consulta, por ejemplo:
                           {
                               "columns": ["id", "nombre", "edad"],
                               "data": [
                                   (1, "Juan", 30),
                                   (2, "María", 25)
                               ]
                           }
        :param estructura_consulta: (Opcional) Diccionario con la estructura de consulta interpretada.
                                    Esto puede usarse para ajustar el mensaje (por ejemplo, si la acción es 'contar'
                                    o 'promedio').
        :return: Cadena de texto con la respuesta formateada.
        """
        if not resultados:
            return "No se encontraron resultados o se produjo un error en la consulta."

        # Si se conoce la acción (por ejemplo, contar o promedio) se puede personalizar el mensaje.
        accion = ""
        if estructura_consulta and "accion" in estructura_consulta:
            accion = estructura_consulta["accion"].lower()

        # Si se trata de una acción que devuelve un valor único (como contar o promedio),
        # se espera que los resultados tengan una única columna y una única fila.
        if accion in ["contar", "promedio"]:
            if resultados.get("data") and len(resultados["data"]) == 1 and len(resultados["data"][0]) == 1:
                valor = resultados["data"][0][0]
                if accion == "contar":
                    return f"El número de registros es: {valor}."
                elif accion == "promedio":
                    return f"El valor promedio es: {valor}."
        
        # En caso de que se trate de una consulta que retorna múltiples registros (por ejemplo, listar),
        # formateamos los resultados como una tabla.
        columnas = resultados.get("columns", [])
        datos = resultados.get("data", [])

        # Construir encabezado.
        header = " | ".join(columnas)
        separator = "-" * len(header)
        filas = [header, separator]

        # Construir cada fila de resultados.
        for fila in datos:
            fila_str = " | ".join(str(item) for item in fila)
            filas.append(fila_str)

        respuesta = "\n".join(filas)
        return respuesta
