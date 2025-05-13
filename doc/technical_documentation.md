# Documentación Técnica del Proyecto EV Challenge

En este documento presento un resumen del proyecto, el enfoque para realizar el pipeline para analizar la base de datos de poblacion de vehiculos electricos, su arquitectura y los principales desafios.

---

## 1. Resumen del proyecto

En este desafío construí un pipeline de datos completo para analizar la poblacion de vehículos eléctricos, desde la recolección automática del CSV de la página web oficial del estado de Washington hasta la generación del dashboard en Power BI

* **Enfoque**: Con la ayuda de chat gpt y claude comencé diseñando la estructura del proyecto, logrando un flujo ETL modular (Extract → Transform → Load).
* **Principales desafíos y Soluciones aplicadas**: Al comenzar con el proyecto decidí comenzar haciendo un analisis de la base de datos con un notebook que es donde más estoy acostumbrado a trabajar. Acá fue donde comencé a encontrar los primeros desafios, donde perdi el mayor tiempo, si bien el dataset es bastante amigable, la columna 'Electric Range' tiene muchos datos que no tiene sentido, como puede ser que me encontré que para el mismo vehículo(mismo año, fabricante y modelo) el dato es diferente. Si bien esta columna no es importante para mi analisis, si lo es la columna 'Clean Alternative Fuel Vehicle (CAFV) Eligibilit' la cual depende de la anterior para definir si es elegible o no. Debido a que el analisis me tomaba mucho tiempo y si bien modifica note que las tendencia se mantenian, por lo tanto opte por no modificar las filas. Sumado a que el desafio es orientado a Data Engineer desidi no realizar analisis de los datos sino que hacer simplemente una limpieza para que luego puedan ser tratados.
Otro de los mayores desafios que me encontre fue la aplicacion de tecnologias que no manejo con comodidad, como Power BI y PostgreSQL y es claro que se va a ver reflejado en los dashboards y su calidad.


## 2. Estructura del Repositorio

```text
ev-data-pipeline/
├── dashboard                # dashboard de Power BI
├── data/
│   ├── raw/                 # Datos crudos descargados
│   └── processed/           # Datos procesados y para Power BI
│       └── power_bi/        # Datos procesador para Power BI
├── doc/                     # Documentacion tecnica
├── logs/                    # Archivos de registro
├── notebooks/               # Notebook para analisis exploratorio
├── src/
│   ├── config.py            # Configuraciones centralizadas
│   ├── database.py          # Operaciones de base de datos
│   ├── extract.py           # Extracción de datos
│   ├── transform.py         # Transformación de datos
│   ├── load.py              # Carga en base de datos
│   ├── powerbi_prep.py      # Preparación para Power BI
│   └── main.py              # Script principal
├── requirements.txt         # Dependencias del entorno virtual
└── README.md                # Este archivo
```

---

## 3. Arquitectura del Pipeline

1. **Extract** (`extract.py`): descarga el CSV desde la URL configurada y lo valida.
2. **Transform** (`transform.py`): lee el CSV, limpia los nombres de columnas, convierte tipos, maneja nulos y duplicados, y guarda resultados en `data/processed/`.
3. **Load** (`load.py`): conecta a PostgreSQL, prepara el dataset para que tenga coincidencia entra las columnas del dataset con la tabla de PostgreSQL y finalmente carga los datos en la tabla de PostgreSQL.
4. **Config** (`config.py`): Centraliza las configuracion, ya sea de las rutas del proyecto, el logging, la base de datos.
5. **Database** (`database.py`): En este script se centraliza las funciones necesarias para PostgreSQL, la conexion, la creacion de la tabla, y las consultas.
6. **Preparacion de Power BI** (`powerbi_prep.py`): Realiza las consultas sql para poder responder las preguntas solicitadas.
7. **Funcion principal**(`main.py`): Esta es la funcion principal, donde se realizan todos los pasos del pipeline



