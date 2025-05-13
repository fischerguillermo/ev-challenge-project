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
        # Log de las columnas disponibles para referencia
        logger.info(f"Columnas disponibles: {df.columns.tolist()}")
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
    rename_dict = {
    'vin_1_10': 'vin',
    'clean_alternative_fuel_vehicle_cafv_eligibility': 'cafv_eligibility'
    }
    
    logger.info("Limpiando nombres de columnas")
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_').str.replace('(', '').str.replace(')', '')
    df = df.rename(columns=rename_dict)
    return df

def select_relevant_columns(df):
    """
    Selecciona sólo las columnas relevantes para el análisis requerido.
    
    Args:
        df (pd.DataFrame): DataFrame completo
        
    Returns:
        pd.DataFrame: DataFrame con columnas seleccionadas
    """
    # Definir columnas requeridas para las preguntas analíticas
    required_columns = [
        'dol_vehicle_id', 'county', 'city', 'state', 'postal_code', 
        'model_year', 'make', 'model', 'electric_vehicle_type',
        'cafv_eligibility', 'electric_range'
    ]
    
    # Verificar qué columnas requeridas existen en el dataset
    available_columns = [col for col in required_columns if col in df.columns]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.warning(f"Las siguientes columnas requeridas no están en el dataset: {missing_columns}")
    
    logger.info(f"Seleccionando {len(available_columns)} columnas relevantes de {len(df.columns)} totales")
    return df[available_columns]

def convert_data_types(df):
    """
    Convierte las columnas a los tipos de datos adecuados.
    
    Args:
        df (pd.DataFrame): DataFrame con los datos
        
    Returns:
        pd.DataFrame: DataFrame con los tipos de datos correctos
    """
    logger.info("Convirtiendo tipos de datos")
    
    # Diccionario de mapeo de columnas a tipos de datos
    type_mapping = {
        'dol_vehicle_id': 'numeric',
        'county': 'category', 
        'city': 'category', 
        'state':'category', 
        'postal_code': 'numeric', 
        'model_year':'date', 
        'make': 'category', 
        'model': 'category', 
        'electric_vehicle_type': 'category',
        'cafv_eligibility': 'category', 
        'electric_range':'numeric'
    }
    
    try:
        # Aplicar conversiones según el tipo
        for col, data_type in type_mapping.items():
            if col in df.columns:
                if data_type == 'numeric':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    logger.info(f"Columna {col} convertida a numérica")
                elif data_type == 'category':
                    df[col] = df[col].astype('category')
                    logger.info(f"Columna {col} convertida a categórica")
                elif data_type == 'date':
                    df[col] = pd.to_datetime(df[col].astype(str) + '-01-01', errors='coerce')
                    logger.info(f"Columna {col} convertida a fecha")
            else:
                logger.warning(f"Columna {col} no encontrada en el dataset")
        
        return df
    
    except Exception as e:
        logger.error(f"Error al convertir tipos de datos: {e}")
        return df

def handle_missing_values(df):
    """
    Maneja los valores faltantes en el DataFrame según la columna.
    
    Args:
        df (pd.DataFrame): DataFrame con posibles valores faltantes
        
    Returns:
        pd.DataFrame: DataFrame con valores faltantes tratados
    """
    logger.info("Manejando valores faltantes")
    
    # Registrar valores nulos por columna antes del tratamiento
    null_counts = df.isnull().sum()
    logger.info(f"Valores nulos por columna antes del tratamiento:\n{null_counts}")
    
    # Rellenar valores nulos en electric_range con 0.0 debido a que ya se reyenaba con 0.0 cuuando se desconoce.
    if 'electric_range' in df.columns:
        n_null_er = df['electric_range'].isnull().sum()
        if n_null_er > 0:
            df['electric_range'] = df['electric_range'].fillna(0.0)
            logger.info(f"Columna electric_range: {n_null_er} valores nulos reemplazados con 0.0")
    
    #. Eliminar filas que aún tengan algún nulo, opto por esto porque son muy pocos los nulos
    total_rows_before = len(df)
    df = df.dropna()
    dropped = total_rows_before - len(df)
    logger.info(f"Se eliminaron {dropped} filas que contenían otros valores nulos")


    # Registrar valores nulos después del tratamiento
    null_counts_after = df.isnull().sum()
    logger.info(f"Valores nulos por columna después del tratamiento:\n{null_counts_after}")
    

    return df

def drop_duplicates(df): # No hay valores duplicados en el dataset pero se deja la función por si acaso
    """
    Elimina filas duplicadas del DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame con posibles duplicados
        
    Returns:
        pd.DataFrame: DataFrame sin duplicados
    """
    logger.info(f"Filas antes de eliminar duplicados: {len(df)}")
    
    # Considero DOL_VEHICLE_ID como el ID único para considerar duplicados
    if 'dol_vehicle_id' in df.columns:
        df_no_duplicates = df.drop_duplicates(subset=['dol_vehicle_id'])
        logger.info(f"Duplicados eliminados basados en DOL VEHICLE ID")
    
    logger.info(f"Filas después de eliminar duplicados: {len(df_no_duplicates)}")
    logger.info(f"Se eliminaron {len(df) - len(df_no_duplicates)} filas duplicadas")
    return df_no_duplicates


def save_processed_data(df, file_name='processed_ev_data.csv', save_full=True):
    """
    Guarda el DataFrame procesado en un archivo CSV. 
    Opcionalmente guarda una versión completa con todas las columnas originales.
    
    Args:
        df (pd.DataFrame): DataFrame procesado
        file_name (str): Nombre del archivo para guardar los datos procesados
        save_full (bool): Si es True, guarda también una versión completa de los datos
        
    Returns:
        str: Ruta al archivo guardado, None si hay error
    """
    try:
        # Crear el directorio si no existe
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
        
        # Ruta completa del archivo
        file_path = os.path.join(PROCESSED_DATA_DIR, file_name)
        
        # Guardar el DataFrame optimizado en CSV
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
        
        # Guardar una copia de los datos originales con todas las columnas
        original_df = df.copy()
        
        # Aplicar transformaciones
        df = clean_column_names(df)
        
        # Seleccionar columnas relevantes para optimización
        df = select_relevant_columns(df)
        
        # Continuar con otras transformaciones
        df = convert_data_types(df)
        df = handle_missing_values(df)
        df = drop_duplicates(df)
        
        # Guardar los datos procesados optimizados
        output_file_path = save_processed_data(df)
        
        # También guardar los datos completos para referencia (con todas las columnas)
        original_df = clean_column_names(original_df)
        full_output_path = save_processed_data(original_df, 'full_processed_ev_data.csv')
        
        logger.info("Proceso de transformación completado con éxito")
        logger.info(f"Dataset optimizado: {len(df.columns)} columnas, {len(df)} filas")
        logger.info(f"Dataset completo: {len(original_df.columns)} columnas, {len(original_df)} filas")
        
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
            print("\nResumen de los datos procesados (optimizado):")
            print(f"Filas: {len(df)}")
            print(f"Columnas: {len(df.columns)}")
            print("\nColumnas en el conjunto de datos optimizado:")
            for col in df.columns:
                print(f"- {col}")
            print("\nPrimeras 5 filas:")
            print(df.head())
