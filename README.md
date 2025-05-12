# Pipeline de Datos para Vehículos Eléctricos

Este proyecto implementa un pipeline de datos para ingestar, transformar y analizar datos de la población de vehículos eléctricos. El objetivo es responder preguntas específicas sobre las tendencias y distribuciones de los vehículos eléctricos.

## Estructura del Proyecto

```
ev-data-pipeline/
├── data/
│   ├── raw/         # Datos crudos descargados de la fuente
│   └── processed/   # Datos procesados y listos para análisis
├── notebooks/
│   └── exploratory_analysis.ipynb  # Análisis exploratorio inicial
├── src/
│   ├── __init__.py
│   ├── extract.py   # Código para extraer datos
│   ├── transform.py # Código para transformar datos
│   ├── load.py      # Código para cargar datos en la base de datos
│   └── main.py      # Script principal para ejecutar el pipeline
├── sql/
│   ├── create_tables.sql          # Scripts para crear tablas
│   └── analysis_queries.sql       # Consultas para análisis
├── dashboard/
│   └── ev_analysis.pbix           # Dashboard de Power BI
├── doc/
│   ├── technical_documentation.md # Documentación técnica
│   └── diagrams/                  # Diagramas del proyecto
├── environment.yml                # Dependencias del entorno
└── README.md                      # Este archivo
```

## Requerimientos

- Python 3.9+
- Conda o Miniconda
- PostgreSQL
- Power BI Desktop (para visualizar el dashboard)

## Configuración del Entorno

1. Clonar este repositorio:
   ```
   git clone https://github.com/tu-usuario/ev-data-pipeline.git
   cd ev-data-pipeline
   ```

2. Crear y activar el entorno conda:
   ```
   conda env create -f environment.yml
   conda activate ev-data-pipeline
   ```

3. Configurar la base de datos PostgreSQL:
   - Crear una base de datos llamada `ev_data`
   - Ejecutar los scripts en la carpeta `sql/` para crear las tablas necesarias

## Ejecutando el Pipeline

1. Exploración de datos:
   ```
   jupyter lab notebooks/exploratory_analysis.ipynb
   ```

2. Ejecutar el pipeline completo:
   ```
   python src/main.py
   ```

## Análisis

El proyecto responde las siguientes preguntas:

1. ¿Cuántos vehículos eléctricos están registrados por año?
2. ¿Cuáles son los 10 principales modelos de vehículos eléctricos por recuento de registros?
3. ¿Dónde se concentran geográficamente los vehículos elegibles para CAFV?
4. ¿Cuál es el cambio año tras año en los registros de vehículos eléctricos por condado?

Los resultados detallados se pueden ver en el dashboard de Power BI ubicado en la carpeta `dashboard/`.

## Documentación

Para más detalles sobre la arquitectura, decisiones de diseño y transformaciones de datos, consulte la documentación técnica en `doc/technical_documentation.md`.