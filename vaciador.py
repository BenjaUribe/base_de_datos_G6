import psycopg2

def vaciar_base_analisis():
    tablas = [
        "hechos_ventas",
        "cliente",
        "estudio",
        "genero",
        "juego",
        "sucursal"
    ]
    with psycopg2.connect(
        dbname="base_datos",
        user="postgres",
        password="zawarudo",
        host="localhost",
        port="5432"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {', '.join(tablas)} RESTART IDENTITY CASCADE;")
        conn.commit()
    print("Base de datos de análisis vaciada.")

if __name__ == "__main__":
    vaciar_base_analisis()