USE PRUEBA_OLTP;
	GO

	/* ================================
	   PACIENTES (tabla transaccional)
	================================ */
	IF OBJECT_ID('pacientes', 'U') IS NOT NULL
		DROP TABLE pacientes;
	GO

	CREATE TABLE pacientes (
		id_paciente INT IDENTITY(1,1) PRIMARY KEY,
		codigo_paciente VARCHAR(20) UNIQUE,
		nombre VARCHAR(150),
		sexo CHAR(1),
		fecha_nacimiento DATE
	);
	GO

	/* ================================
	   MEDICOS
	================================ */
	IF OBJECT_ID('medicos', 'U') IS NOT NULL
		DROP TABLE medicos;
	GO

	CREATE TABLE medicos (
		id_medico INT IDENTITY(1,1) PRIMARY KEY,
		codigo_medico VARCHAR(20) UNIQUE,
		nombre_medico VARCHAR(150),
		especialidad VARCHAR(100)
	);
	GO

	/* ================================
	   DIAGNOSTICOS (CIE10)
	================================ */
	IF OBJECT_ID('diagnosticos', 'U') IS NOT NULL
		DROP TABLE diagnosticos;
	GO

	CREATE TABLE diagnosticos (
		id_diagnostico INT IDENTITY(1,1) PRIMARY KEY,
		codigo_cie10 VARCHAR(10),
		descripcion_cie10 VARCHAR(255),
		grupo_cie10 VARCHAR(150)
	);
	GO

	/* ================================
	   ATENCIONES (tabla de hechos OLTP)
	================================ */
	IF OBJECT_ID('atenciones', 'U') IS NOT NULL
		DROP TABLE atenciones;
	GO

	CREATE TABLE atenciones (
		id_atencion INT IDENTITY(1,1) PRIMARY KEY,
		id_paciente INT NOT NULL,
		id_medico INT NOT NULL,
		id_diagnostico INT NOT NULL,
		fecha DATE NOT NULL,
		hora TIME NOT NULL,
		flag_ira BIT DEFAULT 0,

		CONSTRAINT fk_atencion_paciente FOREIGN KEY (id_paciente) REFERENCES pacientes(id_paciente),
		CONSTRAINT fk_atencion_medico FOREIGN KEY (id_medico) REFERENCES medicos(id_medico),
		CONSTRAINT fk_atencion_diagnostico FOREIGN KEY (id_diagnostico) REFERENCES diagnosticos(id_diagnostico)
	);
	GO