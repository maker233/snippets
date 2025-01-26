from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
from langchain_community.utilities import SQLDatabase
import pandas as pd
from typing import Any
from sqlalchemy import create_engine, text

from langflow.custom import CustomComponent


class DynamicSQLAnalyzerComponent(CustomComponent):
    display_name = "Dynamic SQL Analyzer"
    description = "Analyze and format SQL data dynamically for any schema"
    name = "DynamicSQLAnalyzer"
    beta: bool = True

    def build_config(self):
        return {
            "database_url": {
                "display_name": "Database URL",
                "info": "The URL of the database.",
                "value": "MSQLDATABASE_URL",
            },
            "table_name": {
                "display_name": "Table Name",
                "info": "Name of the table to analyze (e.g., wpta_comments)",
                "value": "wpta_comments",
                "required": True,
            },
            "limit": {
                "display_name": "Limit",
                "info": "Number of records to retrieve",
                "value": 10,
            },
            "format_type": {
                "display_name": "Format Type",
                "info": "How to format the data",
                "options": ["structured", "natural", "technical"],
                "value": "structured",
            }
        }

    def execute_query(self, engine, query: str) -> pd.DataFrame:
        """Ejecuta la query y devuelve un DataFrame"""
        try:
            print(f"Ejecutando query: {query}")
            return pd.read_sql_query(query, engine)
        except Exception as e:
            print(f"Error en execute_query: {str(e)}")
            return pd.DataFrame()

    def format_data(self, df: pd.DataFrame, format_type: str) -> str:
        """Formatea los datos según el tipo especificado"""
        if df.empty:
            return "No se encontraron datos en la consulta."

        if format_type == "structured":
            text = "DATOS ESTRUCTURADOS:\n\n"
            for idx, row in df.iterrows():
                text += f"Registro #{idx + 1}:\n"
                for col in df.columns:
                    text += f"  {col}: {row[col]}\n"
                text += "\n"
                
        elif format_type == "natural":
            text = "RESUMEN EN LENGUAJE NATURAL:\n\n"
            text += f"Encontré {len(df)} registros con {len(df.columns)} columnas.\n\n"
            for idx, row in df.iterrows():
                text += f"En el registro {idx + 1}, "
                descriptions = []
                for col in df.columns:
                    val = str(row[col])
                    if len(val) > 100:  # Truncar textos largos
                        val = val[:100] + "..."
                    descriptions.append(f"el {col} es '{val}'")
                text += ", ".join(descriptions) + ".\n"
                
        else:  # technical
            text = "ANÁLISIS TÉCNICO:\n\n"
            text += f"Dimensiones: {df.shape}\n"
            text += f"Columnas: {', '.join(df.columns)}\n\n"
            text += "Estadísticas:\n"
            for col in df.columns:
                text += f"\n{col}:\n"
                text += f"  - Tipo: {df[col].dtype}\n"
                text += f"  - Valores únicos: {df[col].nunique()}\n"
                text += f"  - Nulos: {df[col].isnull().sum()}\n"
        
        return text

    def build(
        self,
        database_url: str,
        table_name: str,
        limit: int = 10,
        format_type: str = "structured",
        **kwargs,
    ) -> str:
        try:
            print(f"Conectando a la base de datos: {database_url}")
            
            # Crear engine con opciones específicas para MySQL
            engine = create_engine(
                database_url,
                pool_recycle=3600,
                pool_pre_ping=True,
                connect_args={'charset': 'utf8mb4'}
            )
            
            # Verificar que la tabla existe
            verify_query = f"SELECT 1 FROM {table_name} LIMIT 1"
            try:
                self.execute_query(engine, verify_query)
                print(f"Tabla {table_name} verificada correctamente")
            except Exception as e:
                return f"Error: La tabla {table_name} no existe o no es accesible. {str(e)}"
            
            # Construir y ejecutar la query principal
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE 1=1
                LIMIT {limit}
            """
            
            df = self.execute_query(engine, query)
            
            if df.empty:
                return f"No se encontraron datos en la tabla {table_name}."
            
            # Formatear datos
            formatted_text = self.format_data(df, format_type)
            
            print(f"Datos formateados exitosamente en formato {format_type}")
            return formatted_text

        except Exception as e:
            error_msg = f"Error al procesar los datos: {str(e)}"
            print(error_msg)
            return error_msg