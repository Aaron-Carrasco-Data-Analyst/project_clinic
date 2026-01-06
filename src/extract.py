from src.conexion_sql import ConexionSQL

def extract_to_bronze():
    conn = ConexionSQL("oltp")
    conn.conectar("PRUEBA_OLTP")
    
    query = """
    SELECT a.id_atencion, a.fecha, a.hora, a.flag_ira,
           p.id_paciente, p.nombre, p.sexo, p.fecha_nacimiento,
           m.id_medico, m.nombre_medico, m.especialidad,
           d.id_diagnostico, d.codigo_cie10, d.descripcion_cie10, d.grupo_cie10
    FROM atenciones a
    JOIN pacientes p ON a.id_paciente = p.id_paciente
    JOIN medicos m ON a.id_medico = m.id_medico
    JOIN diagnosticos d ON a.id_diagnostico = d.id_diagnostico
    """

    df = conn.ejecutar_select_a_pd(query)
    conn.cerrar_conexion()

    # Guardar en Bronze (Parquet)
    df.to_parquet("data/bronze/atenciones.parquet", index=False)
    print("✅ Datos guardados en Bronze (Parquet)")

extract_to_bronze()