import os
from pathlib import Path
import logging
from dotenv import load_dotenv




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
# Carga las variables del .env
load_dotenv()

DB_CONFIG = {
    'host':     os.getenv('DB_HOST'),
    'port':     os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user':     os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
}

# URL del conjunto de datos de vehículos eléctricos
EV_DATA_URL = 'https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD'
RAW_DATA_FILENAME = 'electric_vehicle_population_data.csv'

# Logger para usar en otros módulos
logger = logging.getLogger(__name__)
