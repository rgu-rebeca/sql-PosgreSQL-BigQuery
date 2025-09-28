--Punto 2 de la practica: Creacion de tablas y restricciones de la base de dato Keepcoding

CREATE TABLE usuarios
(
	usuario_id SERIAL PRIMARY KEY,
	nombre VARCHAR(100) NOT NULL,
	email VARCHAR(100) UNIQUE NOT NULL,
	contrasena VARCHAR(100) NOT NULL,
	fecha_registro DATE NOT NULL DEFAULT CURRENT_DATE
);
--DATE DEFAULT CURRENT_DATE para que si no se pone nada, por defecto se rellene con fecha actual

CREATE TABLE profesores
(
	profesor_id SERIAL PRIMARY KEY,
	nombre VARCHAR(100) NOT NULL,
	email VARCHAR(100) UNIQUE NOT NULL,
	telefono VARCHAR(20) NOT NULL
);

CREATE TABLE categorias
(
	categoria_id SERIAL PRIMARY KEY,
	nombre VARCHAR(50) NOT NULL
);

CREATE TABLE cursos 
(
    curso_id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    descripcion TEXT,
    fecha_inicio DATE NOT NULL,
    fecha_fin DATE,
    usuario_id INT REFERENCES usuarios(usuario_id) ON DELETE SET NULL,
    profesor_id INT REFERENCES profesores(profesor_id) ON DELETE SET NULL,
    categoria_id INT REFERENCES categorias(categoria_id) ON DELETE SET NULL
);
/*se añade ON DELETE SET NULL por integridad referencial para borrar los datos 
al que apunta cuando se borra un usuario/profe/categoria en su tabla.*/
CREATE TABLE inscripciones
(
	inscripcion_id SERIAL PRIMARY KEY,
	estado_es_activo BOOLEAN,
	fecha_inscripcion DATE NOT NULL DEFAULT CURRENT_DATE,
	usuario_id INT REFERENCES usuarios(usuario_id) ON DELETE CASCADE,
	curso_id INT REFERENCES cursos(curso_id) ON DELETE CASCADE
);
/* aqui no se usa ON DELETE SET NULL sino ON DELET CASCADE porque una inscripcion 
es dependiente del usuario y del curso entonces si se borra un usuario o un curso, 
entonces el campo ya no deberia existir, consecuentemente se borra el registro de la inscripcion.*/

CREATE TABLE pagos
(
	pago_id SERIAL PRIMARY KEY,
	usuario_id INT REFERENCES usuarios(usuario_id) ON DELETE CASCADE,
	curso_id INT REFERENCES cursos(curso_id) ON DELETE CASCADE,
	monto NUMERIC(10,2) NOT NULL,
	fecha DATE NOT NULL DEFAULT CURRENT_DATE,
	metodo_pago VARCHAR(30) NOT NULL
);

CREATE TABLE evaluaciones
(
	evaluacion_id SERIAL PRIMARY KEY,
	curso_id INT REFERENCES cursos(curso_id) ON DELETE CASCADE,
	usuario_id INT REFERENCES usuarios(usuario_id) ON DELETE CASCADE,
	nombre VARCHAR(30) NOT NULL,
	fecha DATE NOT NULL DEFAULT CURRENT_DATE,
	tipo VARCHAR(20) CHECK(tipo IN ('practica', 'examen')),
	aprobado BOOLEAN
);
/*aqui uso CHECK(tipo IN ('practica', 'examen')) en vez de dos columnas de booleano para evitar
combinaciones que no tengan setnido como TRUE en practica y TRUE en exmane o FALSE en ambos.*/

CREATE TABLE materiales
(
	material_id SERIAL PRIMARY KEY,
	curso_id INT REFERENCES cursos(curso_id) ON DELETE CASCADE,
	es_video BOOLEAN NOT NULL,
	es_documento BOOLEAN NOT NULL,
	es_enlace BOOLEAN NOT NULL,
	url TEXT
);
