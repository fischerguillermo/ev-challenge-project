# Documentación Técnica del Proyecto EV Challenge

En este documento presento un resumen del proyecto, el enfoque para realizar el pipeline para analizar la base de datos de poblacion de vehiculos electricos, su arquitectura y los principales desafios.

---

## 1. Resumen del proyecto

En este desafío construí un pipeline de datos completo para analizar la población de vehículos eléctricos, desde la recolección automática del CSV de la página web oficial del estado de Washington hasta la generación del dashboard en Power BI que responde a las preguntas planteadas.

### Enfoque
- Diseñé una estructura de proyecto modular siguiendo el flujo ETL (Extract → Transform → Load)
- Creé componentes independientes para cada fase del proceso, facilitando el mantenimiento y futuras ampliaciones
- Implementé validaciones en distintas etapas para garantizar la integridad de los datos

### Principales desafíos y soluciones aplicadas
- **Inconsistencia en datos críticos**: Identifiqué discrepancias en la columna 'Electric Range', donde el mismo modelo de vehículo presentaba valores diferentes. Esto afectaba la columna 'Clean Alternative Fuel Vehicle (CAFV) Eligibility', crucial para el análisis. Tras un análisis exploratorio mediante notebooks, verifiqué que estas inconsistencias no alteraban significativamente las tendencias generales.

- **Optimización del alcance**: Considerando la naturaleza del desafío orientado a ingeniería de datos, prioricé la construcción de un pipeline robusto y funcional por encima del análisis detallado de anomalías en los datos. Implementé transformaciones básicas para garantizar la calidad suficiente para responder las preguntas analíticas.

- **Curva de aprendizaje tecnológica**: Aunque PostgreSQL y Power BI no eran mis herramientas habituales, logré implementar una solución completa que satisface los requerimientos, ampliando mis competencias técnicas en el proceso.

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

## 3. Componentes Principales

1. **Extracción** (`extract.py`): descarga el CSV desde la URL configurada y lo valida.

2. **Transformación** (`transform.py`): lee el CSV, limpia los nombres de columnas, convierte tipos, maneja nulos y duplicados, y guarda resultados en `data/processed/`.

3. **Carga** (`load.py`): conecta a PostgreSQL, prepara el dataset para que tenga coincidencia entra las columnas del dataset con la tabla de PostgreSQL y finalmente carga los datos en la tabla de PostgreSQL.

4. **Configuración** (`config.py`): Centraliza las configuracion, ya sea de las rutas del proyecto, el logging, la base de datos.

5. **Operaciones de Base de Datos** (`database.py`): En este script se centraliza las funciones necesarias para PostgreSQL, la conexion, la creacion de la tabla, y las consultas.

6. **Preparacion de Power BI** (`powerbi_prep.py`): Realiza las consultas sql para poder responder las preguntas solicitadas.

7. **Orquestación**(`main.py`): Función principal, donde se realizan todos los pasos del pipeline
El pipeline implementado sigue una estructura modular donde cada componente cumple una función específica y bien definida:


