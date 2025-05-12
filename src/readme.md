# Análisis de Población de Vehículos Eléctricos

Este proyecto implementa un pipeline de datos para analizar la población de vehículos eléctricos, como parte de la prueba técnica para Promtior.

## Estructura del Proyecto

```
ev_challenge_project/
├── data/
│   ├── raw/                 # Datos crudos descargados
│   └── processed/           # Datos procesados y para Power BI
├── logs/                    # Archivos de registro
├── src/
│   ├── __init__.py
│   ├── config.py            # Configuraciones centralizadas
│   ├── database.py          # Operaciones de base de datos
│   ├── extract.py           # Extracción de datos
│   ├── transform.py         # Transformación de datos
│   ├── load.py              # Carga en base de datos
│   ├── powerbi_prep.py      # Preparación para Power BI
│   └── main.py              # Script principal
└── README.md                # Este archivo
```

## Requisitos

- Python 3.8+
- PostgreSQL

## Instalación

1. Clonar este repositorio:
```bash
git clone https://github.com/tu-usuario/ev_challenge_project.git
cd ev_challenge_project
```

2. Crear un entorno virtual (opcional pero recomendado):
```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

3. Instalar dependencias:
```bash
pip install -r requirements.txt
```

4. Configurar PostgreSQL:
   - Asegúrate de tener PostgreSQL instalado y en ejecución
   - Si es necesario, actualiza las credenciales de la base de datos en `src/config.py`

## Uso

### Ejecutar Pipeline Completo

```bash
cd src
python main.py
```

Este comando ejecutará todo el pipeline:
1. Inicialización de la base de datos
2. Extracción de datos
3. Transformación
4. Carga en PostgreSQL
5. Preparación de datos para Power BI

### Ejecutar Componentes Individuales

También puedes ejecutar cada componente por separado:

```bash
# Inicializar la base de datos
python database.py

# Extraer datos
python extract.py

# Transformar datos (requiere datos extraídos)
python transform.py

# Cargar datos en la base de datos (requiere datos transformados)
python load.py

# Preparar datos para Power BI (requiere datos cargados en la DB)
python powerbi_prep.py
```

## Análisis en Power BI

Para visualizar los datos en Power BI:

1. **Opción 1 - Conexión directa a PostgreSQL**:
   - Abre Power BI Desktop
   - Selecciona "Obtener datos" > "PostgreSQL"
   - Configura la conexión con los parámetros en `config.py`
   - Importa las tablas necesarias

2. **Opción 2 - Archivos CSV**:
   - Los archivos CSV generados se encuentran en `data/processed/power_bi/`
   - En Power BI Desktop, selecciona "Obtener datos" > "Texto/CSV"
   - Importa los archivos CSV generados

## Preguntas Respondidas

El pipeline y el dashboard de Power BI están diseñados para responder a las siguientes preguntas:

1. ¿Cuántos vehículos eléctricos están registrados por año?
2. ¿Cuáles son los 10 modelos de vehículos eléctricos principales por conteo de registros?
3. ¿Dónde se concentran geográficamente los vehículos elegibles para CAFV?
4. ¿Cuál es el cambio año tras año en los registros de vehículos eléctricos por condado?

## Solución de Problemas

- **Error de conexión a PostgreSQL**: Verifica las credenciales en `config.py` y asegúrate de que PostgreSQL esté en ejecución.
- **Error de dependencias**: Asegúrate de haber instalado todas las dependencias con `pip install -r requirements.txt`.
- **Archivos no encontrados**: Verifica que los directorios existan y tengan permisos de escritura.

## Autor

[Tu nombre]
