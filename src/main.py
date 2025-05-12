import os
import time
from config import logger
from database import initialize_database
from extract import extract_data
from transform import transform_data
from load import load_data_to_database
from powerbi_prep import save_query_results

def run_pipeline():
    """
    Ejecuta el pipeline completo de ETL para datos de vehículos eléctricos.
    
    Returns:
        bool: True si el pipeline se ejecutó correctamente, False en caso contrario
    """
    start_time = time.time()
    logger.info("Iniciando pipeline de análisis de vehículos eléctricos")
    
    try:
        # Paso 1: Inicializar la base de datos
        logger.info("Paso 1/5: Inicializando base de datos")
        initialize_database()
        
        # Paso 2: Extraer datos
        logger.info("Paso 2/5: Extrayendo datos")
        raw_file_path = extract_data()
        if not raw_file_path:
            logger.error("Fallo en la extracción de datos. Deteniendo el pipeline.")
            return False
        
        # Paso 3: Transformar datos
        logger.info("Paso 3/5: Transformando datos")
        df, processed_file_path = transform_data(raw_file_path)
        if df is None or processed_file_path is None:
            logger.error("Fallo en la transformación de datos. Deteniendo el pipeline.")
            return False
        
        # Paso 4: Cargar datos a la base de datos
        logger.info("Paso 4/5: Cargando datos en la base de datos")
        load_success = load_data_to_database(df)
        if not load_success:
            logger.error("Fallo en la carga de datos. Deteniendo el pipeline.")
            return False
        
        # Paso 5: Preparar datos para Power BI
        logger.info("Paso 5/5: Preparando datos para Power BI")
        query_results = save_query_results()
        if not query_results:
            logger.warning("No se generaron todos los resultados para Power BI")
        
        # Pipeline completado
        execution_time = time.time() - start_time
        logger.info(f"Pipeline completado con éxito en {execution_time:.2f} segundos")
        
        # Mostrar resumen
        print("\n" + "="*50)
        print("RESUMEN DEL PIPELINE")
        print("="*50)
        print(f"1. Datos extraídos: {os.path.basename(raw_file_path)}")
        print(f"2. Datos procesados: {len(df)} filas, {len(df.columns)} columnas")
        print(f"3. Datos cargados en la base de datos: Éxito")
        print(f"4. Consultas generadas para Power BI: {len(query_results)}")
        print(f"Tiempo total de ejecución: {execution_time:.2f} segundos")
        print("="*50)
        print("\nAhora puedes usar Power BI para conectarte a la base de datos")
        print("o importar los archivos CSV generados en el directorio 'data/processed/power_bi'")
        print("="*50 + "\n")
        
        return True
        
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"Error en el pipeline: {e}")
        logger.info(f"Pipeline interrumpido después de {execution_time:.2f} segundos")
        return False

if __name__ == "__main__":
    run_pipeline()
