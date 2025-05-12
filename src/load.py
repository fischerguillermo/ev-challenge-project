import pandas as pd
import psycopg2
from io import StringIO
from database import get_connection
from config import logger

def load_data_to_database(df, table_name='electric_vehicles'):
    """
    Carga los datos del DataFrame a la tabla especificada en la base de datos PostgreSQL.
    
    Args:
        df (pd.DataFrame): DataFrame con los datos procesados
        table_name (str): Nombre de la tabla en la base de datos
        
    Returns:
        bool: True si la carga fue exitosa, False en caso contrario
    """
    if df is None or df.empty:
        logger.error("No hay datos para cargar en la base de datos")
        return False
    
    connection = None
    try:
        # Establecer conexión
        connection = get_connection()
        cursor = connection.cursor()
        
        # Truncar la tabla si existe para evitar duplicados (opcional)
        logger.info(f"Eliminando datos existentes de la tabla {table_name}")
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        
        # Verificar y ajustar los nombres de columnas si es necesario
        df_copy = prepare_dataframe_for_db(df, table_name, cursor)
        
        # Usar copy_from para carga eficiente
        logger.info(f"Cargando {len(df_copy)} filas en la tabla {table_name}")
        
        # Convertir DataFrame a CSV en memoria
        buffer = StringIO()
        df_copy.to_csv(buffer, index=False, header=False, na_rep='NULL')
        buffer.seek(0)
        
        # Copiar del buffer a la tabla
        cursor.copy_from(buffer, table_name, sep=',', null='NULL', columns=df_copy.columns.tolist())
        
        # Confirmar la transacción
        connection.commit()
        logger.info(f"Datos cargados exitosamente en la tabla {table_name}")
        return True
    
    except psycopg2.Error as e:
        logger.error(f"Error al cargar datos en la base de datos: {e}")
        if connection:
            connection.rollback()
        return False
    
    except Exception as e:
        logger.error(f"Error inesperado durante la carga de datos: {e}")
        if connection:
            connection.rollback()
        return False
    
    finally:
        if connection:
            connection.close()

def prepare_dataframe_for_db(df, table_name, cursor):
    """
    Prepara el DataFrame para la carga en la base de datos, asegurando
    que las columnas coincidan con las de la tabla.
    
    Args:
        df (pd.DataFrame): DataFrame con los datos a cargar
        table_name (str): Nombre de la tabla en la base de datos
        cursor: Cursor de la conexión a la base de datos
        
    Returns:
        pd.DataFrame: DataFrame ajustado para la carga
    """
    try:
        # Obtener las columnas de la tabla en la base de datos
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        db_columns = [desc[0] for desc in cursor.description]
        
        logger.info(f"Columnas en la base de datos: {db_columns}")
        logger.info(f"Columnas en el DataFrame: {df.columns.tolist()}")
        
        # Crear una copia del DataFrame para no modificar el original
        df_copy = df.copy()
        
        # Convertir nombres de columnas a minúsculas para comparación
        df_copy.columns = df_copy.columns.str.lower()
        
        # Solo mantener las columnas que existen en la tabla
        common_columns = [col for col in df_copy.columns if col in db_columns]
        df_copy = df_copy[common_columns]
        
        # Verificar si faltan columnas requeridas
        missing_columns = [col for col in db_columns if col not in common_columns and col != 'id']
        if missing_columns:
            logger.warning(f"Columnas faltantes en el DataFrame: {missing_columns}")
            # Añadir columnas faltantes con valores nulos
            for col in missing_columns:
                df_copy[col] = None
        
        # Asegurar el orden correcto de las columnas (excluyendo 'id' si es autoincremental)
        db_columns_without_id = [col for col in db_columns if col != 'id']
        df_copy = df_copy[db_columns_without_id]
        
        return df_copy
    
    except Exception as e:
        logger.error(f"Error al preparar DataFrame para la base de datos: {e}")
        raise

def load_data_from_file(file_path, table_name='electric_vehicles'):
    """
    Carga datos desde un archivo CSV procesado a la base de datos.
    
    Args:
        file_path (str): Ruta al archivo CSV procesado
        table_name (str): Nombre de la tabla en la base de datos
        
    Returns:
        bool: True si la carga fue exitosa, False en caso contrario
    """
    try:
        logger.info(f"Leyendo datos procesados desde {file_path}")
        df = pd.read_csv(file_path)
        
        if df.empty:
            logger.warning("El archivo CSV está vacío")
            return False
        
        logger.info(f"Archivo leído correctamente. Filas: {len(df)}")
        return load_data_to_database(df, table_name)
    
    except Exception as e:
        logger.error(f"Error al cargar datos desde archivo: {e}")
        return False

if __name__ == "__main__":
    # Si se ejecuta directamente este script
    import os
    from config import PROCESSED_DATA_DIR
    from database import initialize_database
    
    # Asegurarse que la base de datos está inicializada
    initialize_database()
    
    # Buscar el archivo procesado más reciente
    processed_files = [f for f in os.listdir(PROCESSED_DATA_DIR) if f.endswith('.csv')]
    
    if processed_files:
        # Ordenar por fecha de modificación, el más reciente primero
        processed_files.sort(key=lambda x: os.path.getmtime(os.path.join(PROCESSED_DATA_DIR, x)), reverse=True)
        latest_file = os.path.join(PROCESSED_DATA_DIR, processed_files[0])
        
        logger.info(f"Cargando el archivo más reciente: {latest_file}")
        success = load_data_from_file(latest_file)
        
        if success:
            logger.info("Carga de datos completada con éxito")
        else:
            logger.error("Falló la carga de datos")
    else:
        logger.warning(f"No se encontraron archivos CSV procesados en {PROCESSED_DATA_DIR}")
