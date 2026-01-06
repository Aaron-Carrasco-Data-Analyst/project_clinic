USE PRUEBA
GO

CREATE TABLE diagnosticos_clinica.dim_pacientes (
    id_paciente INT IDENTITY(1,1) PRIMARY KEY,
    codigo_paciente VARCHAR(20) NOT NULL,
    sexo VARCHAR(1),
    fecha_nacimiento DATE,
    grupo_edad VARCHAR(20)
);

CREATE TABLE diagnosticos_clinica.dim_medicos (
    id_medico INT IDENTITY(1,1) PRIMARY KEY,
    codigo_medico VARCHAR(20) NOT NULL,
    nombre_medico VARCHAR(150),
    especialidad VARCHAR(100)
);

CREATE TABLE diagnosticos_clinica.dim_diagnosticos_cie10 (
    id_diagnostico INT IDENTITY(1,1) PRIMARY KEY,
    codigo_cie10 VARCHAR(10) NOT NULL,
    descripcion_cie10 VARCHAR(255),
    grupo_cie10 VARCHAR(150)
);

CREATE TABLE diagnosticos_clinica.dim_calendario (
    id_calendario INT IDENTITY(1,1) PRIMARY KEY,
    fecha DATE NOT NULL,
    anio INT,
    mes INT,
    nombre_mes VARCHAR(20),
    semana INT,
    dia INT,
    dia_semana VARCHAR(20),
    trimestre INT
);

CREATE TABLE diagnosticos_clinica.fact_auditoria_medica (
    id_fact INT IDENTITY(1,1) PRIMARY KEY,
    id_calendario INT,
    id_paciente INT,
    id_medico INT,
    id_diagnostico INT,
    hora_atencion TIME,
    cantidad_atenciones INT,
    flag_ira BIT
);