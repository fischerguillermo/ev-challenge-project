# Pipeline de Datos para Vehículos Eléctricos

Este proyecto implementa un pipeline de datos para ingestar, transformar y analizar datos de la población de vehículos eléctricos. El objetivo es responder preguntas específicas sobre las tendencias y distribuciones de los vehículos eléctricos.

## Estructura del Proyecto

```
ev-data-pipeline/
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

## Requerimientos

- Python 3.9+
- Conda o Miniconda
- PostgreSQL
- Power BI Desktop (para visualizar el dashboard)

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

5. Crear el archivo `.env`

En la raíz del proyecto, crea un archivo llamado **`.env`** (asegúrate de que esté en tu `.gitignore`) con el siguiente contenido:

```dotenv
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ev_challenge_db
DB_USER=postgres
DB_PASSWORD=[TuContraseña]
```

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

Los resultados detallados se pueden ver en el dashboard de Power BI ubicado en la carpeta `dashboard/`.

## Documentación

Para más detalles sobre la arquitectura, decisiones de diseño y transformaciones de datos, consulte la documentación técnica en `doc/technical_documentation.md`.