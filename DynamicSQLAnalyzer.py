from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
from langchain_community.utilities import SQLDatabase
import pandas as pd
from typing import Any
from sqlalchemy import create_engine, text
from langflow.schema import Data
import json

from langflow.custom import CustomComponent


class DynamicSQLAnalyzerComponent(CustomComponent):
    display_name = "Dynamic SQL Analyzer"
    description = "Analiza datos SQL y devuelve un objeto Data para procesamiento posterior"
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
                "info": "Name of the table to analyze",
                "value": "wpta_comments",
                "required": True,
            },
            "custom_query": {
                "display_name": "Custom Query",
                "info": "SQL query personalizada. Si se proporciona, ignorará table_name y limit",
                "value": "",
                "required": False,
                "multiline": True,
            },
            "content_columns": {
                "display_name": "Content Columns",
                "info": "Columnas que contienen el contenido principal (separadas por coma)",
                "value": "comment_content",
                "required": True,
            },
            "context_columns": {
                "display_name": "Context Columns",
                "info": "Columnas que proporcionan contexto y deben mantenerse con el contenido (separadas por coma)",
                "value": "comment_post_ID,comment_author,comment_date",
                "required": True,
            },
            "batch_size": {
                "display_name": "Batch Size",
                "info": "Número de registros a procesar por lote",
                "value": 1000,
            },
            "limit": {
                "display_name": "Limit",
                "info": "Límite total de registros (0 para todos)",
                "value": 0,
            }
        }

    def execute_query(self, engine, query: str, chunksize: int = None) -> pd.DataFrame:
        """Ejecuta la query y devuelve un DataFrame o un iterator de DataFrames"""
        try:
            print(f"Ejecutando query: {query}")
            if chunksize:
                return pd.read_sql_query(query, engine, chunksize=chunksize)
            return pd.read_sql_query(query, engine)
        except Exception as e:
            print(f"Error en execute_query: {str(e)}")
            return pd.DataFrame()

    def format_batch(self, df: pd.DataFrame, content_cols: list, context_cols: list) -> str:
        """Formatea un lote de datos manteniendo la relación contenido-contexto"""
        formatted_text = ""
        
        for _, row in df.iterrows():
            # Preparar el contexto que se mantendrá con cada contenido
            context = {col: str(row[col]) for col in context_cols}
            context_str = json.dumps(context, ensure_ascii=False)
            
            # Procesar cada columna de contenido
            for content_col in content_cols:
                content = str(row[content_col])
                if content.strip():  # Solo procesar si hay contenido
                    formatted_text += f"CONTEXT: {context_str}\nCONTENT: {content}\n===\n"
        
        return formatted_text

    def build(
        self,
        database_url: str,
        table_name: str,
        content_columns: str,
        context_columns: str,
        custom_query: str = "",
        batch_size: int = 1000,
        limit: int = 0,
        **kwargs,
    ) -> Data:
        try:
            print(f"Conectando a la base de datos: {database_url}")
            
            engine = create_engine(
                database_url,
                pool_recycle=3600,
                pool_pre_ping=True,
                connect_args={'charset': 'utf8mb4'}
            )
            
            # Preparar las listas de columnas
            content_cols = [col.strip() for col in content_columns.split(',')]
            context_cols = [col.strip() for col in context_columns.split(',')]
            
            # Construir la query
            if custom_query.strip():
                query = custom_query
            else:
                columns = list(set(content_cols + context_cols))
                query = f"""
                    SELECT {', '.join(columns)}
                    FROM {table_name}
                    WHERE 1=1
                    {f'LIMIT {limit}' if limit > 0 else ''}
                """
            
            # Procesar los datos en lotes
            formatted_text = ""
            total_records = 0
            
            for chunk_df in self.execute_query(engine, query, chunksize=batch_size):
                if chunk_df.empty:
                    continue
                
                formatted_text += self.format_batch(chunk_df, content_cols, context_cols)
                total_records += len(chunk_df)
                
                print(f"Procesados {total_records} registros...")
                
                if limit > 0 and total_records >= limit:
                    break
            
            if not formatted_text:
                return Data(text="No se encontraron datos para la consulta especificada.")
            
            metadata = {
                "query_type": "custom" if custom_query.strip() else "default",
                "table_name": table_name,
                "num_records": total_records,
                "content_columns": content_cols,
                "context_columns": context_cols,
                "source": "sql_database",
                "query": query
            }
            
            return Data(text=formatted_text, data=metadata)

        except Exception as e:
            error_msg = f"Error al procesar los datos: {str(e)}"
            print(error_msg)
            return Data(text=error_msg)