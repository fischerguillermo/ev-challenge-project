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
    Crea las tablas necesarias en la base de datos.
    """
    connection = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        
        # Crear tabla para vehículos eléctricos
        create_table_query = """
        CREATE TABLE IF NOT EXISTS electric_vehicles (
            id SERIAL PRIMARY KEY,
            vin VARCHAR(50),
            county VARCHAR(100),
            city VARCHAR(100),
            state VARCHAR(50),
            postal_code VARCHAR(20),
            model_year INTEGER,
            make VARCHAR(100),
            model VARCHAR(100),
            electric_vehicle_type VARCHAR(100),
            cafv_eligibility VARCHAR(100),
            electric_range INTEGER,
            base_msrp NUMERIC,
            legislative_district INTEGER,
            dol_vehicle_id VARCHAR(100),
            vehicle_location VARCHAR(255),
            electric_utility VARCHAR(100),
            registration_date DATE
        );
        """
        cursor.execute(create_table_query)
        
        # Confirmar cambios
        connection.commit()
        logger.info("Tablas creadas correctamente")
        
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

if __name__ == "__main__":
    # Si este script se ejecuta directamente, inicializa la base de datos
    initialize_database()
