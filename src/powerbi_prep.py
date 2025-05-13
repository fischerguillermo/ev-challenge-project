import pandas as pd
import psycopg2
from database import get_connection
from config import PROCESSED_DATA_DIR, logger
import os

def execute_query(query):
    """
    Ejecuta una consulta SQL y devuelve los resultados como DataFrame.
    
    Args:
        query (str): Consulta SQL a ejecutar
        
    Returns:
        pd.DataFrame: DataFrame con los resultados de la consulta
    """
    connection = None
    try:
        connection = get_connection()
        logger.info("Ejecutando consulta SQL")
        
        # Ejecutar consulta y devolver como DataFrame
        df = pd.read_sql_query(query, connection)
        logger.info(f"Consulta ejecutada con éxito. Filas obtenidas: {len(df)}")
        return df
    
    except psycopg2.Error as e:
        logger.error(f"Error al ejecutar consulta: {e}")
        return None
    
    except Exception as e:
        logger.error(f"Error inesperado al ejecutar consulta: {e}")
        return None
    
    finally:
        if connection:
            connection.close()

def get_vehicles_by_year():
    """
    Obtiene el conteo de vehículos eléctricos registrados por año.
    
    Returns:
        pd.DataFrame: DataFrame con el conteo por año
    """
    query = """
    SELECT 
        EXTRACT(YEAR FROM model_year)::INT AS registration_year,
        COUNT(*) AS vehicle_count
    FROM 
        electric_vehicles
    WHERE 
        model_year IS NOT NULL
    GROUP BY 
        registration_year
    ORDER BY 
        registration_year;
    """
    logger.info("Consultando vehículos por año")
    return execute_query(query)

def get_top_models():
    """
    Obtiene los 10 modelos de vehículos eléctricos más registrados.
    
    Returns:
        pd.DataFrame: DataFrame con los 10 modelos principales
    """
    query = """
    SELECT 
        make, 
        model, 
        COUNT(*) AS registration_count
    FROM 
        electric_vehicles
    GROUP BY 
        make, model
    ORDER BY 
        registration_count DESC
    LIMIT 10;
    """
    logger.info("Consultando top 10 modelos")
    return execute_query(query)

def get_cafv_by_location():
    """
    Obtiene la concentración geográfica de vehículos elegibles para CAFV.
    
    Returns:
        pd.DataFrame: DataFrame con conteo por ubicación
    """
    query = """
    SELECT 
        'United States'        AS country,
        county, 
        city, 
        cafv_eligibility,
        COUNT(*) AS vehicle_count
    FROM 
        electric_vehicles
    WHERE 
        cafv_eligibility = 'Clean Alternative Fuel Vehicle Eligible'
    GROUP BY 
        county, city, cafv_eligibility
    ORDER BY 
        vehicle_count DESC;
    """
    logger.info("Consultando concentración geográfica de vehículos CAFV")
    return execute_query(query)

def get_yoy_change():
    """
    Obtiene el cambio año tras año en los registros de vehículos eléctricos por condado.
    
    Returns:
        pd.DataFrame: DataFrame con cambio interanual por condado
    """
    query = """
    WITH yearly_registrations AS (
        SELECT 
            county,
            EXTRACT(YEAR FROM model_year)::INT AS year,
            COUNT(*) AS registration_count
        FROM 
            electric_vehicles
        WHERE 
            model_year IS NOT NULL
        GROUP BY 
            county, year
    ),
    yearly_changes AS (
        SELECT 
            yr1.county,
            yr1.year,
            yr1.registration_count,
            yr2.registration_count AS prev_year_count,
            yr1.registration_count - COALESCE(yr2.registration_count, 0) AS absolute_change,
            CASE 
                WHEN COALESCE(yr2.registration_count, 0) = 0 THEN NULL
                ELSE ROUND(((yr1.registration_count - yr2.registration_count)::numeric / yr2.registration_count) * 100, 2)
            END AS percentage_change
        FROM 
            yearly_registrations yr1
        LEFT JOIN 
            yearly_registrations yr2 
        ON 
            yr1.county = yr2.county AND yr1.year = yr2.year + 1
    )
    SELECT 
        county,
        year,
        registration_count,
        prev_year_count,
        absolute_change,
        percentage_change
    FROM 
        yearly_changes
    ORDER BY 
        county, year;
    """
    logger.info("Consultando cambio interanual por condado")
    return execute_query(query)

def save_query_results():
    """
    Ejecuta todas las consultas y guarda los resultados en archivos CSV.
    
    Returns:
        dict: Diccionario con rutas a los archivos guardados
    """
    # Asegurarse que el directorio existe
    output_dir = os.path.join(PROCESSED_DATA_DIR, 'power_bi')
    os.makedirs(output_dir, exist_ok=True)
    
    results = {}
    
    # Ejecutar y guardar cada consulta
    try:
        # Vehículos por año
        df_by_year = get_vehicles_by_year()
        if df_by_year is not None:
            file_path = os.path.join(output_dir, 'vehicles_by_year.csv')
            df_by_year.to_csv(file_path, index=False)
            results['vehicles_by_year'] = file_path
            logger.info(f"Resultados guardados en {file_path}")
        
        # Top 10 modelos
        df_top_models = get_top_models()
        if df_top_models is not None:
            file_path = os.path.join(output_dir, 'top_models.csv')
            df_top_models.to_csv(file_path, index=False)
            results['top_models'] = file_path
            logger.info(f"Resultados guardados en {file_path}")
        
        # Concentración CAFV
        df_cafv = get_cafv_by_location()
        if df_cafv is not None:
            file_path = os.path.join(output_dir, 'cafv_by_location.csv')
            df_cafv.to_csv(file_path, index=False)
            results['cafv_by_location'] = file_path
            logger.info(f"Resultados guardados en {file_path}")
        
        # Cambio interanual
        df_yoy = get_yoy_change()
        if df_yoy is not None:
            file_path = os.path.join(output_dir, 'yoy_change.csv')
            df_yoy.to_csv(file_path, index=False)
            results['yoy_change'] = file_path
            logger.info(f"Resultados guardados en {file_path}")
        
        logger.info("Todos los resultados de consultas guardados correctamente")
        return results
    
    except Exception as e:
        logger.error(f"Error al guardar resultados de consultas: {e}")
        return results

if __name__ == "__main__":
    # Si se ejecuta directamente este script
    results = save_query_results()
    
    if results:
        logger.info("Datos preparados para Power BI")
        for query_name, file_path in results.items():
            logger.info(f"  - {query_name}: {file_path}")
    else:
        logger.warning("No se generaron resultados para Power BI")
