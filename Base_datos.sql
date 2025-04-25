CREATE TABLE "hechos_ventas" (
  "id_compra" serial PRIMARY KEY,
  "id_juego" integer,
  "fecha_compra" timestamp,
  "precio_clp" integer,
  "id_cliente" integer,
  "id_sucursal" integer,
  "id_estudio" integer
);

CREATE TABLE "juego" (
  "id_juego" serial PRIMARY KEY,
  "nombre" string,
  "formato" string,
  "genero" string,
  "multiplayer" bool
);

CREATE TABLE "cliente" (
  "id_cliente" serial PRIMARY KEY,
  "nombre" string,
  "fecha_nacimiento" timestamp,
  "sexo" varchar
);

CREATE TABLE "sucursal" (
  "id_sucursal" serial PRIMARY KEY,
  "comuna" string,
  "region" string
);

CREATE TABLE "estudio" (
  "id_estudio" serial PRIMARY KEY,
  "nombre" string
);

ALTER TABLE "hechos_ventas" ADD FOREIGN KEY ("id_juego") REFERENCES "juego" ("id_juego");

ALTER TABLE "hechos_ventas" ADD FOREIGN KEY ("id_estudio") REFERENCES "estudio" ("id_estudio");

ALTER TABLE "hechos_ventas" ADD FOREIGN KEY ("id_sucursal") REFERENCES "sucursal" ("id_sucursal");

ALTER TABLE "hechos_ventas" ADD FOREIGN KEY ("id_cliente") REFERENCES "cliente" ("id_cliente");
