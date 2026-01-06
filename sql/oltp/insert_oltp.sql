	/* ================================
	   PACIENTES
	================================ */
	TRUNCATE TABLE pacientes;

	INSERT INTO pacientes (codigo_paciente, nombre, sexo, fecha_nacimiento)
	VALUES
	('P001','Juan Pérez','M','1985-03-12'),
	('P002','María López','F','1990-07-25'),
	('P003','Carlos Díaz','M','2001-11-02'),
	('P004','Ana Torres','F','1975-05-18'),
	('P005','Luis Gómez','M','1968-09-30'),
	('P006','Sofía Ramírez','F','2010-01-15'),
	('P007','Pedro Castillo','M','2005-12-20'),
	('P008','Lucía Fernández','F','1988-04-09'),
	('P009','Miguel Herrera','M','1995-06-22'),
	('P010','Valeria Chávez','F','1979-10-05');
	GO

	/* ================================
	   MEDICOS
	================================ */
	TRUNCATE TABLE medicos;

	INSERT INTO medicos (codigo_medico, nombre_medico, especialidad)
	VALUES
	('M001','Dr. José Medina','Pediatría'),
	('M002','Dra. Carla Rojas','Medicina Interna'),
	('M003','Dr. Alberto Ruiz','Neumología'),
	('M004','Dra. Diana Vega','Medicina General'),
	('M005','Dr. Enrique Salas','Emergencias');
	GO

	/* ================================
	   DIAGNOSTICOS
	================================ */
	TRUNCATE TABLE diagnosticos;

	INSERT INTO diagnosticos (codigo_cie10, descripcion_cie10, grupo_cie10)
	VALUES
	('J00','Resfriado común','IRAs'),
	('J01','Sinusitis aguda','IRAs'),
	('J02','Faringitis aguda','IRAs'),
	('J03','Amigdalitis aguda','IRAs'),
	('J04','Laringitis aguda','IRAs'),
	('A09','Gastroenteritis','Digestivo'),
	('I10','Hipertensión esencial','Crónico'),
	('E11','Diabetes tipo 2','Crónico'),
	('N39','Infección urinaria','Urinario'),
	('K21','Reflujo gastroesofágico','Digestivo');
	GO

	USE PRUEBA_OLTP;
	GO

	-- Insertar 5 registros por cada mes de los últimos 10 meses
	DECLARE @i INT = 0;
	DECLARE @fecha DATE;

	WHILE @i < 10
	BEGIN
		SET @fecha = DATEADD(MONTH, -@i, GETDATE());

		DECLARE @j INT = 1;
		WHILE @j <= 5
		BEGIN
			INSERT INTO atenciones (id_paciente, id_medico, id_diagnostico, fecha, hora, flag_ira)
			VALUES (
				(SELECT TOP 1 id_paciente FROM pacientes ORDER BY NEWID()),   -- ID válido
				(SELECT TOP 1 id_medico FROM medicos ORDER BY NEWID()),       -- ID válido
				(SELECT TOP 1 id_diagnostico FROM diagnosticos ORDER BY NEWID()), -- ID válido
				DATEFROMPARTS(YEAR(@fecha), MONTH(@fecha), @j),              -- Día j del mes
				CAST(CONVERT(VARCHAR(8), DATEADD(MINUTE, ABS(CHECKSUM(NEWID())) % 1440, '00:00'), 108) AS TIME),
				ABS(CHECKSUM(NEWID())) % 2                                   -- flag_ira aleatorio
			);
			SET @j = @j + 1;
		END

		SET @i = @i + 1;
	END
	GO

	-- Insertar datos específicos para enero del 01 al 05
	DECLARE @d INT = 1;
	WHILE @d <= 5
	BEGIN
		INSERT INTO atenciones (id_paciente, id_medico, id_diagnostico, fecha, hora, flag_ira)
		VALUES (
			(SELECT TOP 1 id_paciente FROM pacientes ORDER BY NEWID()),
			(SELECT TOP 1 id_medico FROM medicos ORDER BY NEWID()),
			(SELECT TOP 1 id_diagnostico FROM diagnosticos ORDER BY NEWID()),
			DATEFROMPARTS(YEAR(GETDATE()), 1, @d),   -- Enero 01–05 del año actual
			CAST(CONVERT(VARCHAR(8), DATEADD(MINUTE, ABS(CHECKSUM(NEWID())) % 1440, '00:00'), 108) AS TIME),
			ABS(CHECKSUM(NEWID())) % 2
		);
		SET @d = @d + 1;
	END
	GO