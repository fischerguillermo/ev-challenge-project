import os
from pathlib import Path
import logging

# Configuración de rutas del proyecto
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'
LOGS_DIR = PROJECT_ROOT / 'logs'

# Crear directorios si no existen
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# Configurar logging básico
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / 'ev_pipeline.log'),
        logging.StreamHandler()
    ]
)

# Configuraciones de base de datos
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'ev_challenge_db',
    'user': 'postgres',
    'password': 'fischer10'  # En producción, esto debería manejarse de forma segura
}

# URL del conjunto de datos de vehículos eléctricos
EV_DATA_URL = 'https://data.seattle.gov/api/views/f6w7-q2d6/rows.csv?accessType=DOWNLOAD'
RAW_DATA_FILENAME = 'electric_vehicle_population_data.csv'

# Logger para usar en otros módulos
logger = logging.getLogger(__name__)
