# Lab05 - ETL Pipeline Data Warehouse

Pipeline ETL que integra datos de PostgreSQL (local) y MongoDB (Atlas) en una tabla unificada de Data Warehouse.

## ¿Qué hace?

Extrae datos de:
- PostgreSQL: tablas `pais_envejecimiento` y `pais_poblacion`
- MongoDB: colecciones de `costos_turisticos` y `paises_mundo_big_mac`

Y los integra en una única tabla: `warehouse.paises_turismo`

## Preparar datos (opcional)

Si necesitas crear las tablas e insertar datos en PostgreSQL, ejecuta:

```bash
psql -h localhost -U postgres -d lab05_SQL -f db.sql
```

**Nota:** Asegúrate de ajustar las rutas de los archivos CSV dentro de `db.sql` si están en otra ubicación.

## Cómo ejecutar

### 1. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 2. Configurar credenciales

Crear archivo `.env` con tus credenciales:

```env
# PostgreSQL (local)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=lab05_SQL
POSTGRES_USER=postgres
POSTGRES_PASSWORD=tu_password

# MongoDB Atlas
MONGO_URI=mongodb+srv://usuario:password@cluster.mongodb.net/?retryWrites=true&w=majority
MONGO_DB=lab05

# Data Warehouse (PostgreSQL)
WAREHOUSE_HOST=localhost
WAREHOUSE_PORT=5432
WAREHOUSE_DB=lab05_warehouse
WAREHOUSE_USER=postgres
WAREHOUSE_PASSWORD=tu_password
```

### 3. Ejecutar el pipeline

```bash
python main.py
```

## Resultado

Se crea la tabla `warehouse.paises_turismo` con 19 columnas:

| Columna | Descripción |
|---------|-------------|
| `pais` | Nombre del país |
| `continente` | Continente |
| `region` | Región geográfica |
| `capital` | Capital del país |
| `poblacion` | Población total |
| `tasa_de_envejecimiento` | Tasa de envejecimiento |
| `precio_big_mac_usd` | Precio de Big Mac en USD |
| `hospedaje_bajo_usd` | Costo bajo de hospedaje por día |
| `hospedaje_promedio_usd` | Costo promedio de hospedaje |
| `hospedaje_alto_usd` | Costo alto de hospedaje |
| `comida_bajo_usd` | Costo bajo de comida por día |
| `comida_promedio_usd` | Costo promedio de comida |
| `comida_alto_usd` | Costo alto de comida |
| `transporte_bajo_usd` | Costo bajo de transporte |
| `transporte_promedio_usd` | Costo promedio de transporte |
| `transporte_alto_usd` | Costo alto de transporte |
| `entretenimiento_bajo_usd` | Costo bajo de entretenimiento |
| `entretenimiento_promedio_usd` | Costo promedio de entretenimiento |
| `entretenimiento_alto_usd` | Costo alto de entretenimiento |



### Consultar los datos

```bash
psql -h localhost -U postgres -d lab05_warehouse
```

```sql
SELECT * FROM warehouse.paises_turismo LIMIT 10;
```