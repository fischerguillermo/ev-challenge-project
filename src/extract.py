import os
import requests
from config import RAW_DATA_DIR, RAW_DATA_FILENAME, EV_DATA_URL, logger

def download_ev_data():
    """
    Descarga los datos de vehículos eléctricos desde la URL configurada
    y los guarda en el directorio de datos crudos.
    
    Returns:
        str: Ruta al archivo descargado
    """
    # Ruta completa donde se guardará el archivo
    output_file_path = os.path.join(RAW_DATA_DIR, RAW_DATA_FILENAME)
    
    try:
        # Verificar si el archivo ya existe
        if os.path.exists(output_file_path):
            logger.info(f"El archivo {RAW_DATA_FILENAME} ya existe. Omitiendo descarga.")
            return output_file_path
        
        logger.info(f"Descargando datos desde {EV_DATA_URL}")
        
        # Realizar la solicitud GET
        response = requests.get(EV_DATA_URL, stream=True)
        response.raise_for_status()  # Lanza una excepción si la solicitud falla
        
        # Guardar el archivo
        with open(output_file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Datos descargados correctamente en {output_file_path}")
        return output_file_path
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al descargar los datos: {e}")
        raise
    except IOError as e:
        logger.error(f"Error al guardar el archivo descargado: {e}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado durante la descarga: {e}")
        raise

def validate_file(file_path):
    """
    Valida que el archivo descargado exista y no esté vacío.
    
    Args:
        file_path (str): Ruta al archivo a validar
        
    Returns:
        bool: True si el archivo es válido, False en caso contrario
    """
    try:
        # Verificar que el archivo existe
        if not os.path.exists(file_path):
            logger.error(f"El archivo {file_path} no existe")
            return False
        
        # Verificar que el archivo no está vacío
        if os.path.getsize(file_path) == 0:
            logger.error(f"El archivo {file_path} está vacío")
            return False
        
        logger.info(f"Archivo {file_path} validado correctamente")
        return True
    
    except Exception as e:
        logger.error(f"Error al validar el archivo: {e}")
        return False

def extract_data():
    """
    Función principal que orquesta la extracción de datos.
    
    Returns:
        str: Ruta al archivo de datos crudos si la extracción fue exitosa,
             None en caso contrario
    """
    try:
        # Descargar los datos
        file_path = download_ev_data()
        
        # Validar el archivo descargado
        if validate_file(file_path):
            logger.info("Proceso de extracción completado con éxito")
            return file_path
        else:
            logger.error("Falló la validación del archivo descargado")
            return None
    
    except Exception as e:
        logger.error(f"Error en el proceso de extracción: {e}")
        return None

if __name__ == "__main__":
    # Si este script se ejecuta directamente, realiza la extracción
    extract_data()
