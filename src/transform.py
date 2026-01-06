import os
import logging
import pandas as pd
from src import log_config  # inicializa logging

logger = logging.getLogger(__name__)

def transform_to_silver():
    os.makedirs("data/silver", exist_ok=True)

    # ================================
    # DIM_PACIENTES
    # ================================
    pacientes = pd.read_parquet("data/bronze/pacientes.parquet")
    logger.info("Pacientes cargados desde Bronze (%s filas)", len(pacientes))

    pacientes["codigo_paciente"] = pacientes["codigo_paciente"].astype(str).str.slice(0, 20)
    pacientes["sexo"] = pacientes["sexo"].astype(str).str.slice(0, 1)
    pacientes["fecha_nacimiento"] = pd.to_datetime(pacientes["fecha_nacimiento"], errors="coerce")

    pacientes = pacientes.dropna(subset=["codigo_paciente", "fecha_nacimiento"])
    pacientes = pacientes.drop_duplicates(subset=["codigo_paciente"])
    pacientes = pacientes[pacientes["fecha_nacimiento"] <= pd.Timestamp.today()]

    hoy = pd.Timestamp.today()
    pacientes["edad"] = hoy.year - pacientes["fecha_nacimiento"].dt.year
    pacientes["grupo_edad"] = pd.cut(
        pacientes["edad"],
        bins=[0, 17, 64, 120],
        labels=["Menor", "Adulto", "Mayor"]
    )

    pacientes.to_parquet("data/silver/dim_pacientes.parquet", index=False)
    logger.info("Dim_Pacientes transformada y guardada en Silver (%s filas)", len(pacientes))

    # ================================
    # DIM_MEDICOS
    # ================================
    medicos = pd.read_parquet("data/bronze/medicos.parquet")
    logger.info("Medicos cargados desde Bronze (%s filas)", len(medicos))

    medicos["codigo_medico"] = medicos["codigo_medico"].astype(str).str.slice(0, 20)
    medicos["nombre_medico"] = medicos["nombre_medico"].astype(str).str.slice(0, 150)
    medicos["especialidad"] = medicos["especialidad"].astype(str).str.slice(0, 100)

    medicos = medicos.dropna(subset=["codigo_medico"])
    medicos = medicos.drop_duplicates(subset=["codigo_medico"])

    medicos.to_parquet("data/silver/dim_medicos.parquet", index=False)
    logger.info("Dim_Medicos transformada y guardada en Silver (%s filas)", len(medicos))

    # ================================
    # DIM_DIAGNOSTICOS
    # ================================
    diagnosticos = pd.read_parquet("data/bronze/diagnosticos.parquet")
    logger.info("Diagnosticos cargados desde Bronze (%s filas)", len(diagnosticos))

    diagnosticos["codigo_cie10"] = diagnosticos["codigo_cie10"].astype(str).str.slice(0, 10)
    diagnosticos["descripcion_cie10"] = diagnosticos["descripcion_cie10"].astype(str).str.slice(0, 255)
    diagnosticos["grupo_cie10"] = diagnosticos["grupo_cie10"].astype(str).str.slice(0, 150)

    diagnosticos = diagnosticos.dropna(subset=["codigo_cie10"])
    diagnosticos = diagnosticos.drop_duplicates(subset=["codigo_cie10"])

    diagnosticos.to_parquet("data/silver/dim_diagnosticos.parquet", index=False)
    logger.info("Dim_Diagnosticos transformada y guardada en Silver (%s filas)", len(diagnosticos))

    # ================================
    # DIM_CALENDARIO
    # ================================
    atenciones = pd.read_parquet("data/bronze/atenciones.parquet")
    logger.info("Atenciones cargadas desde Bronze (%s filas)", len(atenciones))

    fechas = pd.DataFrame({"fecha": pd.to_datetime(atenciones["fecha"].unique(), errors="coerce")})
    fechas = fechas.dropna().drop_duplicates()

    fechas["anio"] = fechas["fecha"].dt.year
    fechas["mes"] = fechas["fecha"].dt.month
    fechas["nombre_mes"] = fechas["fecha"].dt.strftime("%B")
    fechas["semana"] = fechas["fecha"].dt.isocalendar().week
    fechas["dia"] = fechas["fecha"].dt.day
    fechas["dia_semana"] = fechas["fecha"].dt.strftime("%A")
    fechas["trimestre"] = fechas["fecha"].dt.quarter

    fechas.to_parquet("data/silver/dim_calendario.parquet", index=False)
    logger.info("Dim_Calendario generada y guardada en Silver (%s filas)", len(fechas))

    # ================================
    # ATENCIONES (para la Fact)
    # ================================
    # Enriquecer con códigos de negocio
    atenciones = atenciones.merge(
        pacientes[["id_paciente", "codigo_paciente"]],
        on="id_paciente",
        how="left"
    )
    atenciones = atenciones.merge(
        medicos[["id_medico", "codigo_medico"]],
        on="id_medico",
        how="left"
    )
    atenciones = atenciones.merge(
        diagnosticos[["id_diagnostico", "codigo_cie10"]],
        on="id_diagnostico",
        how="left"
    )

    atenciones["fecha"] = pd.to_datetime(atenciones["fecha"], errors="coerce")
    # En transform.py
    atenciones["hora"] = atenciones["hora"].astype(str)  # guardar como texto   
    atenciones["flag_ira"] = atenciones["flag_ira"].astype(bool)

    atenciones = atenciones.dropna(subset=["codigo_paciente", "codigo_medico", "codigo_cie10", "fecha"])

    atenciones.to_parquet("data/silver/atenciones.parquet", index=False)
    logger.info("Atenciones transformadas y guardadas en Silver (%s filas)", len(atenciones))

    logger.info("Transformación a Silver finalizada correctamente.")

if __name__ == "__main__":
    transform_to_silver()