# Lab05 - ETL Pipeline para Data Warehouse

Pipeline ETL (Extract, Transform, Load) modular que integra datos de PostgreSQL (local) y MongoDB (Atlas) en un Data Warehouse centralizado con una tabla unificada.

## Descripción del Proyecto

Este proyecto implementa el **Ejercicio 2** del Lab05, que consiste en:

1. **Extracción** de datos de PostgreSQL local (lab05_SQL)
2. **Extracción** de datos de MongoDB Atlas (lab05)
3. **Transformación y limpieza** de datos de ambas fuentes
4. **Integración** de todas las fuentes en **UNA SOLA TABLA** unificada
5. **Carga** de la tabla integrada en un Data Warehouse PostgreSQL

## Estructura del Proyecto

```
Data-Warehouse/
├── config/                     # Configuración de bases de datos
│   ├── __init__.py
│   └── database_config.py
├── extractors/                 # Módulos de extracción
│   ├── __init__.py
│   ├── postgres_extractor.py  # Extractor de PostgreSQL
│   └── mongo_extractor.py     # Extractor de MongoDB
├── transformers/               # Módulos de transformación
│   ├── __init__.py
│   └── data_cleaner.py        # Limpieza de datos
├── integrators/                # Módulos de integración
│   ├── __init__.py
│   └── warehouse_builder.py   # Constructor de tabla unificada
├── loaders/                    # Módulos de carga
│   ├── __init__.py
│   └── warehouse_loader.py    # Cargador al warehouse
├── utils/                      # Utilidades
│   ├── __init__.py
│   └── logger.py              # Sistema de logging
├── main.py                     # Script principal
├── requirements.txt            # Dependencias
├── queries_warehouse.sql      # Queries de ejemplo
├── .env.example               # Ejemplo de configuración
└── README.md                  # Este archivo
```

## Requisitos Previos

- Python 3.8 o superior
- PostgreSQL (local)
- MongoDB Atlas (cuenta y cluster configurado)
- Bases de datos existentes:
  - PostgreSQL: `lab05_SQL` con tablas:
    - `pais_envejecimiento`
    - `pais_poblacion`
  - MongoDB: `lab05` con colecciones:
    - `costos_turisticos_africa`
    - `costos_turisticos_america`
    - `costos_turisticos_asia`
    - `costos_turisticos_europa`
    - `costos_turisticos_mundo`
    - `paises_mundo_big_mac`

## Instalación

### 1. Clonar el repositorio o navegar al directorio

```bash
cd Data-Warehouse
```

### 2. Crear un entorno virtual (recomendado)

```bash
python -m venv venv
```

### 3. Activar el entorno virtual

**Windows:**
```bash
venv\Scripts\activate
```

**Linux/Mac:**
```bash
source venv/bin/activate
```

### 4. Instalar dependencias

```bash
pip install -r requirements.txt
```

## Configuración

### 1. Crear archivo .env

Copiar el archivo `.env.example` a `.env`:

```bash
cp .env.example .env
```

### 2. Editar el archivo .env

Configurar las credenciales de las bases de datos:

```env
# PostgreSQL Configuration (Local)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=lab05_SQL
POSTGRES_USER=postgres
POSTGRES_PASSWORD=tu_password_postgres

# MongoDB Configuration (Atlas)
MONGO_URI=mongodb+srv://usuario:password@cluster.mongodb.net/?retryWrites=true&w=majority
MONGO_DB=lab05

# Data Warehouse Configuration (PostgreSQL)
WAREHOUSE_HOST=localhost
WAREHOUSE_PORT=5432
WAREHOUSE_DB=lab05_warehouse
WAREHOUSE_USER=postgres
WAREHOUSE_PASSWORD=tu_password_warehouse
```

**Importante:**
- Reemplazar credenciales con tus valores reales
- El Data Warehouse se creará automáticamente si no existe

## Uso

### Primera vez: Setup del warehouse (opcional)

Si el pipeline falla al crear el schema, ejecuta primero:

```bash
python setup_warehouse.py
```

Este script crea el schema `warehouse` en la base de datos.

### Ejecutar el pipeline completo

```bash
python main.py
```

### Verificar el Data Warehouse

```bash
python verify_warehouse.py
```

Este script muestra:
- Si la tabla existe
- Número de registros y columnas
- Esquema completo
- Registros por continente
- Última ejecución del ETL

### Salida esperada

```
[STEP 0] Loading configuration...
[STEP 1] Extracting data from PostgreSQL (local)...
[OK] Extracted data from 2 PostgreSQL tables

[STEP 2] Extracting data from MongoDB (Atlas)...
[OK] Extracted data from 6 MongoDB collections

[STEP 3] Cleaning and transforming data...
[OK] Cleaned 2 PostgreSQL tables
[OK] Cleaned 6 MongoDB collections

[STEP 4] Building unified data warehouse table...
[OK] Built unified table: 318 rows, 19 columns

[STEP 5] Loading unified table into Data Warehouse...
[OK] Loaded unified table 'warehouse.paises_turismo'

ETL Pipeline completed successfully!

Summary:
  PostgreSQL tables extracted: 2
  MongoDB collections extracted: 6
  Unified table: warehouse.paises_turismo
  Total rows: 318
  Total columns: 19
```

## Estructura del Data Warehouse

El Data Warehouse contiene **UNA SOLA TABLA** que integra todas las fuentes:

```
lab05_warehouse
└── warehouse/
    ├── paises_turismo          (TABLA PRINCIPAL - 318 registros)
    └── etl_metadata            (tracking de ejecuciones)
```

### Esquema de la tabla `paises_turismo`

```sql
CREATE TABLE warehouse.paises_turismo (
    -- Información geográfica
    pais                        VARCHAR,
    continente                  VARCHAR,
    region                      VARCHAR,
    capital                     VARCHAR,

    -- Demografía
    poblacion                   BIGINT,
    tasa_de_envejecimiento      NUMERIC,

    -- Indicadores económicos
    precio_big_mac_usd          NUMERIC,

    -- Costos de hospedaje (USD por día)
    hospedaje_bajo_usd          NUMERIC,
    hospedaje_promedio_usd      NUMERIC,
    hospedaje_alto_usd          NUMERIC,

    -- Costos de comida (USD por día)
    comida_bajo_usd             NUMERIC,
    comida_promedio_usd         NUMERIC,
    comida_alto_usd             NUMERIC,

    -- Costos de transporte (USD por día)
    transporte_bajo_usd         NUMERIC,
    transporte_promedio_usd     NUMERIC,
    transporte_alto_usd         NUMERIC,

    -- Costos de entretenimiento (USD por día)
    entretenimiento_bajo_usd    NUMERIC,
    entretenimiento_promedio_usd NUMERIC,
    entretenimiento_alto_usd    NUMERIC
);
```

## Características del Pipeline

### 1. Extracción (Extract)

- **PostgresExtractor**:
  - Conecta a PostgreSQL local
  - Extrae todas las tablas automáticamente
  - Manejo de errores robusto
  - Soporta encoding UTF-8 y Latin-1

- **MongoExtractor**:
  - Conecta a MongoDB Atlas
  - Extrae todas las colecciones
  - Convierte ObjectId a string para compatibilidad
  - Manejo de estructuras anidadas

### 2. Transformación (Transform)

El módulo `DataCleaner` realiza:
- Eliminación de duplicados
- Manejo de valores nulos
- Normalización de strings
- Conversión de tipos de datos
- Preserva estructuras anidadas para procesamiento posterior

### 3. Integración (Build)

El módulo `DataWarehouseBuilder`:
- **Aplana estructuras anidadas** de MongoDB
- **Une todas las colecciones** de costos turísticos
- **Integra con tablas SQL** usando el campo `país` como llave
- **Elimina columnas duplicadas** y columnas ID innecesarias
- **Organiza las columnas** en orden lógico
- Genera **UNA TABLA UNIFICADA** lista para análisis

### 4. Carga (Load)

El módulo `WarehouseLoader`:
- Crea la base de datos del warehouse si no existe
- Crea schema `warehouse` para organizar datos
- Usa SQLAlchemy para compatibilidad con pandas
- Carga datos usando bulk insert para eficiencia
- Registra metadata de cada ejecución ETL

## Consultas de Ejemplo

El archivo `queries_warehouse.sql` contiene 15 queries de ejemplo para analizar los datos:

1. Ver primeras 10 filas
2. Contar total de registros
3. Ver esquema de la tabla
4. Registros por continente
5. Top 10 países con mayor población
6. Top 10 Big Mac más caros
7. Países con mayor tasa de envejecimiento
8. Destinos turísticos más económicos
9. Destinos turísticos más caros
10. Estadísticas por continente
11. Metadata de ejecuciones ETL
12. Países con información completa
13. Análisis BigMac vs costos
14. Regiones más económicas
15. Relación población-costos

### Ejemplo de Consulta:

```sql
-- Top 10 destinos turísticos más económicos
SELECT
    pais,
    continente,
    hospedaje_promedio_usd,
    comida_promedio_usd,
    transporte_promedio_usd,
    entretenimiento_promedio_usd,
    (hospedaje_promedio_usd + comida_promedio_usd +
     transporte_promedio_usd + entretenimiento_promedio_usd) as costo_total_diario
FROM warehouse.paises_turismo
WHERE hospedaje_promedio_usd IS NOT NULL
ORDER BY costo_total_diario ASC
LIMIT 10;
```

## Verificar el Data Warehouse

### Opción 1: pgAdmin o psql

```bash
psql -h localhost -U postgres -d lab05_warehouse
```

```sql
\dt warehouse.*
SELECT COUNT(*) FROM warehouse.paises_turismo;
```

### Opción 2: Ejecutar queries del archivo

```bash
psql -h localhost -U postgres -d lab05_warehouse < queries_warehouse.sql
```

## Solución de Problemas

### Error de encoding en PostgreSQL

Si ves errores como `'utf-8' codec can't decode byte`, ejecuta en psql:

```sql
ALTER SYSTEM SET lc_messages TO 'C';
SELECT pg_reload_conf();
```

Luego reinicia PostgreSQL.

### MongoDB no se conecta

- Verifica la URI en `.env`
- Verifica que tu IP esté en la whitelist de Atlas
- Verifica las credenciales del usuario

### No se extraen datos

- Verifica que las bases de datos tengan datos
- Revisa los logs generados (`etl_pipeline_*.log`)
- Verifica las credenciales en `.env`

## Logs

Cada ejecución genera un archivo de log con timestamp:

```
etl_pipeline_YYYYMMDD_HHMMSS.log
```

El log contiene información detallada sobre:
- Conexiones a bases de datos
- Datos extraídos
- Transformaciones aplicadas
- Errores y advertencias
- Resumen final

## Dependencias Principales

- **psycopg2-binary**: Driver PostgreSQL
- **pymongo**: Driver MongoDB
- **pandas**: Procesamiento de datos
- **sqlalchemy**: ORM y engine para pandas
- **python-dotenv**: Manejo de variables de entorno

## Arquitectura ETL

```
┌─────────────────┐     ┌─────────────────┐
│  PostgreSQL     │     │  MongoDB Atlas  │
│  (lab05_SQL)    │     │  (lab05)        │
│  - envejecimiento│     │  - costos_*     │
│  - poblacion    │     │  - big_mac      │
└────────┬────────┘     └────────┬────────┘
         │                       │
         │   EXTRACT             │
         │                       │
         ▼                       ▼
    ┌────────────────────────────────┐
    │      ETL Pipeline (Python)      │
    │  1. Extract (extractors/)       │
    │  2. Clean (transformers/)       │
    │  3. Build (integrators/)        │
    │  4. Load (loaders/)             │
    └────────────────┬───────────────┘
                     │
                     │   LOAD
                     ▼
         ┌──────────────────────┐
         │  Data Warehouse      │
         │  (lab05_warehouse)   │
         │                      │
         │  warehouse.          │
         │  paises_turismo      │
         │  (318 registros)     │
         └──────────────────────┘
```

## Autor

Lab05 - Base de Datos 2
Séptimo Semestre

## Licencia

Este proyecto es para fines educativos.
