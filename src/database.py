import psycopg2
from psycopg2 import sql
from config import DB_CONFIG, logger

def get_connection():
    """
    Establece y retorna una conexión a la base de datos PostgreSQL.
    """
    try:
        connection = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        logger.info("Conexión a la base de datos establecida correctamente")
        return connection
    except psycopg2.Error as e:
        logger.error(f"Error al conectar a la base de datos: {e}")
        raise

def create_database_if_not_exists():
    """
    Crea la base de datos si no existe.
    """
    # Primero conectamos a la base de datos predeterminada 'postgres'
    connection = None
    try:
        # Conectar a la base de datos postgres para poder crear nuestra DB
        tmp_config = DB_CONFIG.copy()
        tmp_config['database'] = 'postgres'
        
        connection = psycopg2.connect(
            host=tmp_config['host'],
            port=tmp_config['port'],
            database=tmp_config['database'],
            user=tmp_config['user'],
            password=tmp_config['password']
        )
        connection.autocommit = True
        cursor = connection.cursor()
        
        # Verificar si la base de datos ya existe
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_CONFIG['database'],))
        exists = cursor.fetchone()
        
        if not exists:
            # Crear base de datos
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(DB_CONFIG['database'])
            ))
            logger.info(f"Base de datos '{DB_CONFIG['database']}' creada correctamente")
        else:
            logger.info(f"Base de datos '{DB_CONFIG['database']}' ya existe")
            
    except psycopg2.Error as e:
        logger.error(f"Error al crear la base de datos: {e}")
        raise
    finally:
        if connection:
            connection.close()

def create_tables():
    """
    Crea las tablas necesarias en la base de datos, optimizadas para las consultas analíticas requeridas.
    """
    connection = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        
        # Crear tabla optimizada con solo las columnas necesarias para el análisis requerido
        # Eliminamos columnas innecesarias para las preguntas específicas
        create_table_query = """
        CREATE TABLE IF NOT EXISTS electric_vehicles (
            id SERIAL PRIMARY KEY,
            dol_vehicle_id NUMERIC,
            county VARCHAR(100),
            city VARCHAR(100),
            state VARCHAR(100),
            postal_code VARCHAR(100),
            model_year DATE,
            make VARCHAR(100),
            model VARCHAR(100),
            electric_vehicle_type VARCHAR(100),
            cafv_eligibility VARCHAR(100),
            electric_range NUMERIC
        );
        
        -- Crear índices para mejorar rendimiento de consultas
        CREATE INDEX IF NOT EXISTS idx_ev_model_year ON electric_vehicles(model_year);
        CREATE INDEX IF NOT EXISTS idx_ev_model ON electric_vehicles(model);
        CREATE INDEX IF NOT EXISTS idx_ev_county ON electric_vehicles(county);
        CREATE INDEX IF NOT EXISTS idx_ev_cafv ON electric_vehicles(cafv_eligibility);
        """
        cursor.execute(create_table_query)
        
        # Confirmar cambios
        connection.commit()
        logger.info("Tablas e índices creados correctamente")
        
    except psycopg2.Error as e:
        logger.error(f"Error al crear las tablas: {e}")
        if connection:
            connection.rollback()
        raise
    finally:
        if connection:
            connection.close()

def initialize_database():
    """
    Inicializa la base de datos creándola si no existe y generando las tablas necesarias.
    """
    create_database_if_not_exists()
    create_tables()
    logger.info("Base de datos inicializada correctamente")

# Función para ejecutar consultas analíticas comunes
def execute_query(query, params=None):
    """
    Ejecuta una consulta SQL y devuelve los resultados.
    
    Args:
        query (str): Consulta SQL a ejecutar
        params (tuple, optional): Parámetros para la consulta
        
    Returns:
        list: Resultados de la consulta
    """
    connection = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
            
        # Obtener resultados
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        
        return {'columns': column_names, 'data': results}
        
    except psycopg2.Error as e:
        logger.error(f"Error al ejecutar consulta: {e}")
        return None
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    # Si este script se ejecuta directamente, inicializa la base de datos
    initialize_database()
