## Proyecto ETL – Clínica San Felipe

Este proyecto implementa un flujo **ETL** completo para mover datos desde un sistema OLTP hacia un Data Warehouse (DW), siguiendo las capas **Bronze → Silver → Staging → Fact**.

### Arquitectura del proceso

1. **Extract (Bronze)**  
   - Se conecta a la base de datos OLTP (`PRUEBA_OLTP`).  
   - Extrae las tablas `pacientes`, `medicos`, `diagnosticos` y `atenciones`.  
   - Los datos se guardan en formato Parquet en `data/bronze/`.  
   - Se puede limitar la extracción con un parámetro `fecha_desde` para traer solo registros recientes de `atenciones`.

2. **Transform (Silver)**  
   - Limpieza y validación de datos.  
   - Generación de dimensiones:  
     - `dim_pacientes` (validación de fechas, derivación de edad y grupo etario).  
     - `dim_medicos` (normalización de nombres y especialidades).  
     - `dim_diagnosticos` (códigos CIE10 y descripciones).  
     - `dim_calendario` (atributos de fecha: año, mes, semana, día, trimestre).  
   - Transformación de `atenciones` enriquecida con **códigos de negocio** (`codigo_paciente`, `codigo_medico`, `codigo_cie10`).  
   - Los resultados se guardan en `data/silver/`.

3. **Load (Staging → DW)**  
   - Se cargan las tablas Silver hacia el esquema `temporal` en el DW (`PRUEBA`).  
   - Se insertan las dimensiones en `diagnosticos_clinica`.  
   - Se realiza la carga incremental de la fact `fact_auditoria_medica`, evitando duplicados mediante `LEFT JOIN`.  
   - La fact utiliza surrogate keys del DW y conserva trazabilidad con los códigos de negocio.

### Organización del código

- `src/extract.py` → extracción desde OLTP a Bronze.  
- `src/transform.py` → transformación de Bronze a Silver.  
- `src/load.py` → carga de Silver a Staging y DW.  
- `src/conexion_sql.py` → clase de conexión a SQL Server.  
- `src/main.py` → orquestador del ETL (ejecuta Extract → Transform → Load).

### Ejecución

1. Activar el entorno virtual:
   ```bash
   env\Scripts\activate      # Windows
   ```

2. Ejecutar el ETL completo:
   ```bash
   python -m src.main
   ```

3. Ejemplo de log esperado:
   ```
   [INFO] __main__: Iniciando ETL desde src/main.py ...
   [INFO] __main__: Extrayendo datos desde 2025-01-06
   [INFO] src.extract: atenciones guardado en Bronze (55 filas)
   [INFO] src.transform: Dim_Calendario generada y guardada en Silver (50 filas)
   [INFO] src.load: Tabla staging temporal.stg_atenciones cargada (55 filas)
   [INFO] __main__: ETL finalizado con éxito
   ```

### Notas

- El parámetro `fecha_desde` en `extract_to_bronze` permite limitar la extracción de atenciones.  
- Se recomienda ejecutar el ETL diariamente con `fecha_desde = hoy - 1` para procesar solo los registros nuevos.  
- El logging está configurado para mostrar cada paso con número de filas procesadas.

---
