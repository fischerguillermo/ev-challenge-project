import os
import pandas as pd
import numpy as np
from datetime import datetime
from config import PROCESSED_DATA_DIR, logger

def read_raw_data(file_path):
    """
    Lee el archivo CSV de datos crudos en un DataFrame de pandas.
    
    Args:
        file_path (str): Ruta al archivo CSV de datos crudos
        
    Returns:
        pd.DataFrame: DataFrame con los datos crudos, None si hay error
    """
    try:
        logger.info(f"Leyendo datos del archivo: {file_path}")
        df = pd.read_csv(file_path)
        logger.info(f"Datos leídos correctamente. Filas: {len(df)}, Columnas: {len(df.columns)}")
        return df
    except Exception as e:
        logger.error(f"Error al leer el archivo CSV: {e}")
        return None

def clean_column_names(df):
    """
    Limpia los nombres de las columnas: los pasa a minúsculas y reemplaza espacios con guiones bajos.
    
    Args:
        df (pd.DataFrame): DataFrame a limpiar
        
    Returns:
        pd.DataFrame: DataFrame con nombres de columnas limpios
    """
    logger.info("Limpiando nombres de columnas")
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
    return df

def convert_data_types(df):
    """
    Convierte las columnas a los tipos de datos adecuados.
    
    Args:
        df (pd.DataFrame): DataFrame con los datos
        
    Returns:
        pd.DataFrame: DataFrame con los tipos de datos correctos
    """
    logger.info("Convirtiendo tipos de datos")
    
    # Mapeo de columnas a tipos de datos
    try:
        # Convertir columnas numéricas
        if 'model_year' in df.columns:
            df['model_year'] = pd.to_numeric(df['model_year'], errors='coerce')
        
        if 'electric_range' in df.columns:
            df['electric_range'] = pd.to_numeric(df['electric_range'], errors='coerce')
        
        if 'base_msrp' in df.columns:
            df['base_msrp'] = pd.to_numeric(df['base_msrp'], errors='coerce')
        
        if 'legislative_district' in df.columns:
            df['legislative_district'] = pd.to_numeric(df['legislative_district'], errors='coerce')
        
        # Convertir fechas
        date_cols = [col for col in df.columns if 'date' in col.lower()]
        for date_col in date_cols:
            try:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                logger.info(f"Columna {date_col} convertida a datetime")
            except Exception as e:
                logger.warning(f"No se pudo convertir la columna {date_col} a datetime: {e}")
        
        return df
    
    except Exception as e:
        logger.error(f"Error al convertir tipos de datos: {e}")
        return df  # Devolver el DataFrame original en caso de error

def handle_missing_values(df):
    """
    Maneja los valores faltantes en el DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame con posibles valores faltantes
        
    Returns:
        pd.DataFrame: DataFrame con valores faltantes tratados
    """
    logger.info("Manejando valores faltantes")
    
    # Contar valores nulos por columna
    null_counts = df.isnull().sum()
    logger.info(f"Valores nulos por columna antes del tratamiento:\n{null_counts}")
    
    # Estrategia simple para valores faltantes
    # Para columnas categóricas: rellenar con 'Desconocido'
    categorical_cols = df.select_dtypes(include=['object']).columns
    for col in categorical_cols:
        df[col] = df[col].fillna('Desconocido')
    
    # Para columnas numéricas: rellenar con la mediana
    numeric_cols = df.select_dtypes(include=['number']).columns
    for col in numeric_cols:
        df[col] = df[col].fillna(df[col].median())
    
    # Para fechas: mantener como NaT
    
    # Verificar valores nulos después del tratamiento
    null_counts_after = df.isnull().sum()
    logger.info(f"Valores nulos por columna después del tratamiento:\n{null_counts_after}")
    
    return df

def drop_duplicates(df):
    """
    Elimina filas duplicadas del DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame con posibles duplicados
        
    Returns:
        pd.DataFrame: DataFrame sin duplicados
    """
    logger.info(f"Filas antes de eliminar duplicados: {len(df)}")
    df_no_duplicates = df.drop_duplicates()
    logger.info(f"Filas después de eliminar duplicados: {len(df_no_duplicates)}")
    logger.info(f"Se eliminaron {len(df) - len(df_no_duplicates)} filas duplicadas")
    return df_no_duplicates

def extract_registration_year(df):
    """
    Extrae el año de registro a partir de la fecha de registro si existe.
    
    Args:
        df (pd.DataFrame): DataFrame con datos de vehículos
        
    Returns:
        pd.DataFrame: DataFrame con columna de año de registro añadida
    """
    try:
        # Buscar columna de fecha de registro
        reg_date_cols = [col for col in df.columns if 'registration' in col.lower() and 'date' in col.lower()]
        
        if reg_date_cols:
            date_col = reg_date_cols[0]
            logger.info(f"Extrayendo año de registro de la columna: {date_col}")
            
            # Asegurarse de que la columna es datetime
            if pd.api.types.is_datetime64_any_dtype(df[date_col]):
                df['registration_year'] = df[date_col].dt.year
                logger.info("Columna registration_year creada correctamente")
            else:
                # Intentar convertir a datetime si no lo es
                try:
                    df['registration_year'] = pd.to_datetime(df[date_col], errors='coerce').dt.year
                    logger.info("Columna registration_year creada después de conversión")
                except Exception as e:
                    logger.warning(f"No se pudo extraer el año de registro: {e}")
        else:
            logger.warning("No se encontró columna de fecha de registro")
        
        return df
    
    except Exception as e:
        logger.error(f"Error al extraer el año de registro: {e}")
        return df

def save_processed_data(df, file_name='processed_ev_data.csv'):
    """
    Guarda el DataFrame procesado en un archivo CSV.
    
    Args:
        df (pd.DataFrame): DataFrame procesado
        file_name (str): Nombre del archivo para guardar los datos procesados
        
    Returns:
        str: Ruta al archivo guardado, None si hay error
    """
    try:
        # Crear el directorio si no existe
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
        
        # Ruta completa del archivo
        file_path = os.path.join(PROCESSED_DATA_DIR, file_name)
        
        # Guardar el DataFrame en CSV
        df.to_csv(file_path, index=False)
        logger.info(f"Datos procesados guardados en: {file_path}")
        
        return file_path
    
    except Exception as e:
        logger.error(f"Error al guardar los datos procesados: {e}")
        return None

def transform_data(input_file_path):
    """
    Función principal que orquesta el proceso de transformación de datos.
    
    Args:
        input_file_path (str): Ruta al archivo de datos crudos
        
    Returns:
        tuple: (DataFrame procesado, ruta al archivo procesado) o (None, None) si hay error
    """
    try:
        # Leer los datos
        df = read_raw_data(input_file_path)
        if df is None:
            return None, None
        
        # Aplicar transformaciones
        df = clean_column_names(df)
        df = convert_data_types(df)
        df = handle_missing_values(df)
        df = drop_duplicates(df)
        df = extract_registration_year(df)
        
        # Guardar los datos procesados
        output_file_path = save_processed_data(df)
        
        logger.info("Proceso de transformación completado con éxito")
        return df, output_file_path
    
    except Exception as e:
        logger.error(f"Error en el proceso de transformación: {e}")
        return None, None

if __name__ == "__main__":
    # Si se ejecuta directamente, necesitamos saber qué archivo procesar
    from extract import extract_data
    
    # Extraer datos si no tenemos el archivo
    raw_file_path = extract_data()
    
    if raw_file_path:
        # Transformar los datos
        df, processed_file_path = transform_data(raw_file_path)
        if df is not None:
            # Mostrar información básica sobre los datos procesados
            print("\nResumen de los datos procesados:")
            print(f"Filas: {len(df)}")
            print(f"Columnas: {len(df.columns)}")
            print("\nPrimeras 5 filas:")
            print(df.head())
