import logging
import datetime

class SQLGenerationAgent:
    """
    Agente encargado de generar una sentencia SQL a partir de una estructura interpretada de consulta.
    
    La estructura de consulta es un diccionario con los siguientes campos:
      - 'accion': acción a realizar (ej. "contar", "listar", "promedio").
      - 'tabla': nombre de la tabla a consultar.
      - 'filtros': diccionario con pares columna-valor que representan condiciones para la consulta.
          Se soportan rangos expresados como diccionarios, por ejemplo:
          {
              "$gte": "25-01-2025",
              "$lte": "02-02-2025"
          }
      - (Opcional) 'columna': para acciones de promedio.
    """
    
    def __init__(self, limit=25):
        self.limit = limit
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def generar_sql(self, estructura, schema):
        action = estructura.get("accion", "").lower()
        table = estructura.get("tabla", "")
        filters = estructura.get("filtros", {})

        if not table:
            self.logger.error("No se especificó la tabla en la estructura de consulta.")
            return None

        if table not in schema:
            self.logger.error("La tabla '%s' no existe en el esquema.", table)
            return None

        where_clauses = []
        for col, val in filters.items():
            # Validar que la columna exista en la tabla
            if col not in schema[table]["columns"]:
                self.logger.warning("La columna '%s' no existe en la tabla '%s'. Se omitirá este filtro.", col, table)
                continue
            
            column_type = schema[table]["columns"][col]["type"].lower()
            # Heurística para determinar si la columna es de fecha/tiempo en formato epoch:
            is_date_field = False
            if "timestamp" in column_type or "datetime" in column_type:
                is_date_field = True
            elif column_type in ["int", "bigint", "mediumint", "smallint"]:
                if "time" in col.lower() or "date" in col.lower():
                    is_date_field = True

            # Si el filtro es un diccionario (por ejemplo, para rangos)
            if isinstance(val, dict):
                if "$gte" in val:
                    try:
                        if is_date_field:
                            date_obj = datetime.datetime.strptime(val["$gte"], "%d-%m-%Y")
                            # Convertir a epoch en milisegundos
                            epoch_val = int(date_obj.timestamp() * 1000)
                            clause = f"`{col}` >= {epoch_val}"
                        else:
                            clause = f"`{col}` >= '{str(val['$gte']).replace('\'', '\'\'')}'"
                    except Exception as e:
                        self.logger.error("Error al procesar $gte en la columna %s: %s", col, e)
                        clause = f"`{col}` >= '{str(val['$gte']).replace('\'', '\'\'')}'"
                    where_clauses.append(clause)
                if "$lte" in val:
                    try:
                        if is_date_field:
                            date_obj = datetime.datetime.strptime(val["$lte"], "%d-%m-%Y")
                            # Convertir a epoch en milisegundos
                            epoch_val = int(date_obj.timestamp() * 1000)
                            clause = f"`{col}` <= {epoch_val}"
                        else:
                            clause = f"`{col}` <= '{str(val['$lte']).replace('\'', '\'\'')}'"
                    except Exception as e:
                        self.logger.error("Error al procesar $lte en la columna %s: %s", col, e)
                        clause = f"`{col}` <= '{str(val['$lte']).replace('\'', '\'\'')}'"
                    where_clauses.append(clause)
            else:
                # Procesar filtro simple
                if isinstance(val, (int, float)):
                    clause = f"`{col}` = {val}"
                else:
                    val_str = str(val).replace("'", "''")
                    clause = f"`{col}` = '{val_str}'"
                where_clauses.append(clause)
        
        where_clause = ""
        if where_clauses:
            where_clause = " WHERE " + " AND ".join(where_clauses)
        
        sql_query = ""
        if action == "contar":
            sql_query = f"SELECT COUNT(*) FROM `{table}`{where_clause} LIMIT {self.limit};"
        elif action == "listar":
            sql_query = f"SELECT * FROM `{table}`{where_clause} LIMIT {self.limit};"
        elif action == "promedio":
            avg_column = estructura.get("columna")
            if not avg_column:
                self.logger.error("Para la acción 'promedio' se requiere especificar la columna a promediar.")
                return None
            if avg_column not in schema[table]["columns"]:
                self.logger.error("La columna '%s' no existe en la tabla '%s'.", avg_column, table)
                return None
            sql_query = f"SELECT AVG(`{avg_column}`) FROM `{table}`{where_clause} LIMIT {self.limit};"
        else:
            self.logger.error("Acción '%s' no reconocida para la generación de SQL.", action)
            return None

        return sql_query
