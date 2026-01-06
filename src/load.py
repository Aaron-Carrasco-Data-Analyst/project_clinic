import logging
import pandas as pd
from sqlalchemy import text
from src.conexion_sql import ConexionSQL
from src import log_config

logger = logging.getLogger(__name__)

def load_to_dw():
    conn = ConexionSQL("dw")
    conn.conectar("PRUEBA")

    silver_tables = {
        "dim_pacientes": "data/silver/dim_pacientes.parquet",
        "dim_medicos": "data/silver/dim_medicos.parquet",
        "dim_diagnosticos": "data/silver/dim_diagnosticos.parquet",
        "dim_calendario": "data/silver/dim_calendario.parquet"
    }

    # Cargar staging en esquema temporal
    for name, path in silver_tables.items():
        df = pd.read_parquet(path)
        df.to_sql(f"stg_{name}", con=conn.engine, schema="temporal", if_exists="replace", index=False)
        logger.info("Tabla staging temporal.stg_%s cargada (%s filas)", name, len(df))


        # ATENCIONES (staging)
    atenciones = pd.read_parquet("data/silver/atenciones.parquet")
    atenciones.to_sql("stg_atenciones", con=conn.engine, schema="temporal", if_exists="replace", index=False)
    logger.info("Tabla staging temporal.stg_atenciones cargada (%s filas)", len(atenciones))
    # DIM_PACIENTES
    insert_pacientes = """
    INSERT INTO diagnosticos_clinica.dim_pacientes (codigo_paciente, sexo, fecha_nacimiento, grupo_edad)
    SELECT s.codigo_paciente, s.sexo, s.fecha_nacimiento, s.grupo_edad
    FROM temporal.stg_dim_pacientes s
    LEFT JOIN diagnosticos_clinica.dim_pacientes d ON d.codigo_paciente = s.codigo_paciente
    WHERE d.codigo_paciente IS NULL;
    """
    with conn.engine.begin() as connection:
        connection.execute(text(insert_pacientes))

    # DIM_MEDICOS
    insert_medicos = """
    INSERT INTO diagnosticos_clinica.dim_medicos (codigo_medico, nombre_medico, especialidad)
    SELECT s.codigo_medico, s.nombre_medico, s.especialidad
    FROM temporal.stg_dim_medicos s
    LEFT JOIN diagnosticos_clinica.dim_medicos d ON d.codigo_medico = s.codigo_medico
    WHERE d.codigo_medico IS NULL;
    """
    with conn.engine.begin() as connection:
        connection.execute(text(insert_medicos))

    # DIM_DIAGNOSTICOS
    insert_diag = """
    INSERT INTO diagnosticos_clinica.dim_diagnosticos_cie10 (codigo_cie10, descripcion_cie10, grupo_cie10)
    SELECT s.codigo_cie10, s.descripcion_cie10, s.grupo_cie10
    FROM temporal.stg_dim_diagnosticos s
    LEFT JOIN diagnosticos_clinica.dim_diagnosticos_cie10 d ON d.codigo_cie10 = s.codigo_cie10
    WHERE d.codigo_cie10 IS NULL;
    """
    with conn.engine.begin() as connection:
        connection.execute(text(insert_diag))

    # DIM_CALENDARIO
    insert_cal = """
    INSERT INTO diagnosticos_clinica.dim_calendario (fecha, anio, mes, nombre_mes, semana, dia, dia_semana, trimestre)
    SELECT s.fecha, s.anio, s.mes, s.nombre_mes, s.semana, s.dia, s.dia_semana, s.trimestre
    FROM temporal.stg_dim_calendario s
    LEFT JOIN diagnosticos_clinica.dim_calendario d ON d.fecha = s.fecha
    WHERE d.fecha IS NULL;
    """
    with conn.engine.begin() as connection:
        connection.execute(text(insert_cal))

    # FACT
    insert_fact = """
INSERT INTO diagnosticos_clinica.fact_auditoria_medica (
    id_calendario, id_paciente, id_medico, id_diagnostico,
    hora_atencion, cantidad_atenciones, flag_ira
)
SELECT dc.id_calendario,
       dp.id_paciente,
       dm.id_medico,
       dd.id_diagnostico,
       CAST(a.hora AS TIME),  
       1,
       a.flag_ira
FROM temporal.stg_atenciones a
INNER JOIN diagnosticos_clinica.dim_calendario dc
        ON a.fecha = dc.fecha
INNER JOIN diagnosticos_clinica.dim_pacientes dp
        ON dp.codigo_paciente = a.codigo_paciente
INNER JOIN diagnosticos_clinica.dim_medicos dm
        ON dm.codigo_medico = a.codigo_medico
INNER JOIN diagnosticos_clinica.dim_diagnosticos_cie10 dd
        ON dd.codigo_cie10 = a.codigo_cie10
LEFT JOIN diagnosticos_clinica.fact_auditoria_medica f
       ON f.id_calendario = dc.id_calendario
      AND f.id_paciente = dp.id_paciente
      AND f.id_medico = dm.id_medico
      AND f.id_diagnostico = dd.id_diagnostico
      AND f.hora_atencion = a.hora
WHERE f.id_fact IS NULL;
    """
    with conn.engine.begin() as connection:
        connection.execute(text(insert_fact))

    conn.cerrar_conexion()
    logger.info("Carga incremental finalizada correctamente.")

if __name__ == "__main__":
    load_to_dw()