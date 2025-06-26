import time
import psycopg2
from datetime import date

ANALISIS = {
    "dbname": "venta_juegos",
    "user": "postgres",
    "password": "zawarudo",
    "host": "localhost",
    "port": "5432"
}
TRANSACCIONAL = {
    "dbname": "venta_juegos_t",
    "user": "postgres",
    "password": "zawarudo",
    "host": "localhost",
    "port": "5432"
}

def vaciar_base_analisis():
    tablas = [
        "hechos_ventas",
        "cliente",
        "estudio",
        "genero",
        "juego",
        "sucursal"
    ]
    with psycopg2.connect(**ANALISIS) as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {', '.join(tablas)} RESTART IDENTITY CASCADE;")
        conn.commit()
    print("Base de datos de análisis vaciada.")

def sincronizar_y_mostrar():
    with psycopg2.connect(**TRANSACCIONAL) as conn_trans, psycopg2.connect(**ANALISIS) as conn_ana:
        cur_trans = conn_trans.cursor()
        cur_ana = conn_ana.cursor()
        cur_aux = conn_trans.cursor()  # Cursor auxiliar para consultas dentro del loop

        # 1. Insertar dimensiones primero

        # Empresas -> Estudio
        cur_trans.execute("SELECT id_empresa, nombre FROM empresas;")
        for id_empresa, nombre in cur_trans.fetchall():
            print(f"Insertando estudio: ({id_empresa}, {nombre})")
            cur_ana.execute("INSERT INTO estudio (id_estudio, nombre) VALUES (%s, %s);", (id_empresa, nombre))

        # Generos -> Genero
        cur_trans.execute("SELECT id_genero, nombre FROM generos;")
        for id_genero, nombre in cur_trans.fetchall():
            print(f"Insertando genero: ({id_genero}, {nombre})")
            cur_ana.execute("INSERT INTO genero (id_genero, nombre_genero) VALUES (%s, %s);", (id_genero, nombre))

        # Sucursales -> Sucursal
        cur_trans.execute("SELECT id_sucursal, comuna, region FROM sucursales;")
        for id_sucursal, comuna, region in cur_trans.fetchall():
            # OJO: comuna es la región y region es la comuna en la transaccional
            print(f"Insertando sucursal: ({id_sucursal}, {region})")
            cur_ana.execute("INSERT INTO sucursal (id_sucursal, nombre_sucursal) VALUES (%s, %s);", (id_sucursal, region))

        # Clientes -> Cliente (id_cliente, nombre, fecha_nacimiento)
        cur_trans.execute("SELECT id_cliente, nombre, fecha_nacimiento FROM clientes;")
        for id_cliente, nombre, fecha_nacimiento in cur_trans.fetchall():
            print(f"Insertando cliente: ({id_cliente}, {nombre}, {fecha_nacimiento})")
            cur_ana.execute(
                "INSERT INTO cliente (id_cliente, nombre, fecha_nacimiento) VALUES (%s, %s, %s);",
                (id_cliente, nombre, fecha_nacimiento)
            )

        # Videojuegos -> Juego (id_juego, nombre)
        cur_trans.execute("SELECT id_videojuego, nombre FROM videojuegos;")
        for id_juego, nombre in cur_trans.fetchall():
            print(f"Insertando juego: ({id_juego}, {nombre})")
            cur_ana.execute("INSERT INTO juego (id_juego, nombre) VALUES (%s, %s);", (id_juego, nombre))

        # Poblar tabla multiplayer en la base analítica si no existe
        for val in [0, 1]:
            cur_ana.execute("INSERT INTO multiplayer (id_multiplayer, multiplayer) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (val, val))
        # Poblar tabla sexo en la base analítica: (1,1) hombre, (2,0) mujer
        cur_ana.execute("INSERT INTO sexo (id_sexo, sexo) VALUES (1, 1) ON CONFLICT DO NOTHING;")
        cur_ana.execute("INSERT INTO sexo (id_sexo, sexo) VALUES (2, 0) ON CONFLICT DO NOTHING;")

        # 2. Ahora insertar en hechos_ventas
        cur_trans.execute("""
            SELECT 
                f.id_factura, 
                f.metodo_pago, 
                df.videojuego_id, 
                c.id_cliente, 
                v.proveedor_id, 
                ps.sucursal_id
            FROM facturas f
            JOIN detallefactura df ON f.id_factura = df.factura_id
            JOIN videojuegos v ON df.videojuego_id = v.id_videojuego
            JOIN proveedor p ON v.proveedor_id = p.id_proveedor
            JOIN proveedorsucursal ps ON p.id_proveedor = ps.proveedor_id
            JOIN clientes c ON f.cliente_id = c.id_cliente
        """)
        for row in cur_trans.fetchall():
            (id_factura, metodo_pago, id_juego, id_cliente, id_proveedor, id_sucursal) = row

            id_estudio = id_proveedor

            # Asegurar que el estudio existe en la analítica
            cur_ana.execute("SELECT 1 FROM estudio WHERE id_estudio = %s", (id_estudio,))
            if not cur_ana.fetchone():
                cur_aux.execute("SELECT nombre FROM empresas WHERE id_empresa = %s", (id_estudio,))
                nombre_estudio = cur_aux.fetchone()[0] if cur_aux.rowcount > 0 else f"Estudio_{id_estudio}"
                cur_ana.execute("INSERT INTO estudio (id_estudio, nombre) VALUES (%s, %s)", (id_estudio, nombre_estudio))

            # Usar el cursor auxiliar para no romper el fetchall del principal
            cur_aux.execute("""
                SELECT genero_id FROM videojuegogenero WHERE videojuego_id = %s
            """, (id_juego,))
            vj = cur_aux.fetchone()
            id_genero = vj[0] if vj else None

            # Obtener los nombres de región y comuna de la sucursal
            cur_aux.execute("""
                SELECT region, comuna FROM sucursales WHERE id_sucursal = %s
            """, (id_sucursal,))
            suc = cur_aux.fetchone()
            region_nombre = suc[0] if suc else None  # region (real)
            comuna_nombre = suc[1] if suc else None  # comuna (real)

            # Buscar el id_region real usando el nombre de la columna comuna de la transaccional
            id_region = None
            if comuna_nombre:
                cur_ana.execute("SELECT id_region FROM region WHERE region_nombre = %s", (comuna_nombre,))
                reg = cur_ana.fetchone()
                id_region = reg[0] if reg else None

            # Buscar el id_comuna real
            id_comuna = None
            if comuna_nombre:
                cur_ana.execute("SELECT id_comuna FROM comuna WHERE comuna_nombre = %s", (comuna_nombre,))
                com = cur_ana.fetchone()
                id_comuna = com[0] if com else None

            # Obtener fecha_compra y precio_clp
            cur_aux.execute("SELECT fecha, monto_factura FROM facturas WHERE id_factura = %s", (id_factura,))
            fact = cur_aux.fetchone()
            fecha_compra = fact[0] if fact else None
            precio_clp = fact[1] if fact else None

            # Obtener fecha_nacimiento y sexo del cliente
            cur_aux.execute("SELECT fecha_nacimiento, sexo FROM clientes WHERE id_cliente = %s", (id_cliente,))
            cli = cur_aux.fetchone()
            fecha_nacimiento = cli[0] if cli else None
            sexo = cli[1] if cli else None

            # Calcular edad y asignar id_rango_etario
            if fecha_nacimiento:
                hoy = date.today()
                edad = hoy.year - fecha_nacimiento.year - ((hoy.month, hoy.day) < (fecha_nacimiento.month, fecha_nacimiento.day))
            else:
                edad = None

            if edad is not None:
                if edad < 10:
                    id_rango_etario = 0
                elif 10 <= edad <= 19:
                    id_rango_etario = 1
                elif 20 <= edad <= 29:
                    id_rango_etario = 2
                elif 30 <= edad <= 39:
                    id_rango_etario = 3
                else:
                    id_rango_etario = 4
            else:
                id_rango_etario = None

            # Asignar id_sexo
            if sexo is not None:
                if sexo.lower() == "hombre":
                    id_sexo = 1
                else:
                    id_sexo = 2
            else:
                id_sexo = None

            # Obtener id_formato, id_multiplayer del videojuego
            cur_aux.execute("SELECT formato, multiplayer FROM videojuegos WHERE id_videojuego = %s", (id_juego,))
            vj2 = cur_aux.fetchone()
            formato = vj2[0] if vj2 else None
            multiplayer_val = vj2[1] if vj2 else None

            # Convertir formato a entero: 1 = Fisico, 2 = Digital
            if formato is not None:
                if formato.lower() == "fisico":
                    id_formato = 1
                elif formato.lower() == "digital":
                    id_formato = 2
                else:
                    id_formato = None
            else:
                id_formato = None

            # Obtener id_multiplayer real desde la tabla multiplayer de análisis
            id_multiplayer = None
            if multiplayer_val is not None:
                cur_ana.execute("SELECT id_multiplayer FROM multiplayer WHERE multiplayer = %s", (multiplayer_val,))
                res = cur_ana.fetchone()
                id_multiplayer = res[0] if res else None

            print(f"""
Insertando hechos_ventas:
    id_compra      = {id_factura}
    id_juego       = {id_juego}
    metodo_pago    = {metodo_pago}
    id_cliente     = {id_cliente}
    id_sucursal    = {id_sucursal}
    id_estudio     = {id_estudio}
    id_genero      = {id_genero}
    id_region      = {id_region}
    id_comuna      = {id_comuna}
    fecha_compra   = {fecha_compra}
    precio_clp     = {precio_clp}
    id_rango_etario= {id_rango_etario}
    id_sexo        = {id_sexo}
    id_formato     = {id_formato}
    id_multiplayer  = {id_multiplayer}
""")
            cur_ana.execute("""
                INSERT INTO hechos_ventas (
                    id_compra, id_juego, fecha_compra, precio_clp, id_cliente, id_sucursal, id_estudio, id_rango_etario, id_sexo, id_formato, id_multiplayer, id_genero, id_region, id_comuna
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                id_factura, id_juego, fecha_compra, precio_clp, id_cliente, id_sucursal, id_estudio, id_rango_etario, id_sexo, id_formato, id_multiplayer, id_genero, id_region, id_comuna
            ))

        conn_ana.commit()
        print("Sincronización completada.")

if __name__ == "__main__":
    try:
        while True:
            vaciar_base_analisis()
            sincronizar_y_mostrar()
            print("Esperando 60 segundos para la próxima sincronización...\n")
            time.sleep(60)
    except KeyboardInterrupt:
        print("Sincronización detenida por el usuario.")

import psycopg2
from datetime import date
import time

ANALISIS = {
    "dbname": "venta_juegos",
    "user": "postgres",
    "password": "123",
    "host": "localhost",
    "port": "5432"
}
TRANSACCIONAL = {
    "dbname": "venta_juegos_t",
    "user": "postgres",
    "password": "123",
    "host": "localhost",
    "port": "5432"
}

def vaciar_base_analisis():
    tablas = [
        "hechos_ventas",
        "cliente",
        "estudio",
        "genero",
        "juego",
        "sucursal"
    ]
    with psycopg2.connect(**ANALISIS) as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {', '.join(tablas)} RESTART IDENTITY CASCADE;")
        conn.commit()
    print("Base de datos de análisis vaciada.")

def sincronizar_y_mostrar():
    with psycopg2.connect(**TRANSACCIONAL) as conn_trans, psycopg2.connect(**ANALISIS) as conn_ana:
        cur_trans = conn_trans.cursor()
        cur_ana = conn_ana.cursor()
        cur_aux = conn_trans.cursor()  # Cursor auxiliar para consultas dentro del loop

        # 1. Insertar dimensiones primero

        # Empresas -> Estudio
        cur_trans.execute("SELECT id_empresa, nombre FROM empresas;")
        for id_empresa, nombre in cur_trans.fetchall():
            print(f"Insertando estudio: ({id_empresa}, {nombre})")
            cur_ana.execute("INSERT INTO estudio (id_estudio, nombre) VALUES (%s, %s);", (id_empresa, nombre))

        # Generos -> Genero
        cur_trans.execute("SELECT id_genero, nombre FROM generos;")
        for id_genero, nombre in cur_trans.fetchall():
            print(f"Insertando genero: ({id_genero}, {nombre})")
            cur_ana.execute("INSERT INTO genero (id_genero, nombre_genero) VALUES (%s, %s);", (id_genero, nombre))

        # Sucursales -> Sucursal
        cur_trans.execute("SELECT id_sucursal, comuna, region FROM sucursales;")
        for id_sucursal, comuna, region in cur_trans.fetchall():
            # OJO: comuna es la región y region es la comuna en la transaccional
            print(f"Insertando sucursal: ({id_sucursal}, {region})")
            cur_ana.execute("INSERT INTO sucursal (id_sucursal, nombre_sucursal) VALUES (%s, %s);", (id_sucursal, region))

        # Clientes -> Cliente (id_cliente, nombre, fecha_nacimiento)
        cur_trans.execute("SELECT id_cliente, nombre, fecha_nacimiento FROM clientes;")
        for id_cliente, nombre, fecha_nacimiento in cur_trans.fetchall():
            print(f"Insertando cliente: ({id_cliente}, {nombre}, {fecha_nacimiento})")
            cur_ana.execute(
                "INSERT INTO cliente (id_cliente, nombre, fecha_nacimiento) VALUES (%s, %s, %s);",
                (id_cliente, nombre, fecha_nacimiento)
            )

        # Videojuegos -> Juego (id_juego, nombre)
        cur_trans.execute("SELECT id_videojuego, nombre FROM videojuegos;")
        for id_juego, nombre in cur_trans.fetchall():
            print(f"Insertando juego: ({id_juego}, {nombre})")
            cur_ana.execute("INSERT INTO juego (id_juego, nombre) VALUES (%s, %s);", (id_juego, nombre))

        # Poblar tabla multiplayer en la base analítica si no existe
        for val in [0, 1]:
            cur_ana.execute("INSERT INTO multiplayer (id_multiplayer, multiplayer) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (val, val))
        # Poblar tabla sexo en la base analítica: (1,1) hombre, (2,0) mujer
        cur_ana.execute("INSERT INTO sexo (id_sexo, sexo) VALUES (1, 1) ON CONFLICT DO NOTHING;")
        cur_ana.execute("INSERT INTO sexo (id_sexo, sexo) VALUES (2, 0) ON CONFLICT DO NOTHING;")

        # 2. Ahora insertar en hechos_ventas
        cur_trans.execute("""
            SELECT 
                f.id_factura, 
                f.metodo_pago, 
                df.videojuego_id, 
                c.id_cliente, 
                v.proveedor_id, 
                ps.sucursal_id
            FROM facturas f
            JOIN detallefactura df ON f.id_factura = df.factura_id
            JOIN videojuegos v ON df.videojuego_id = v.id_videojuego
            JOIN proveedor p ON v.proveedor_id = p.id_proveedor
            JOIN proveedorsucursal ps ON p.id_proveedor = ps.proveedor_id
            JOIN clientes c ON f.cliente_id = c.id_cliente
        """)
        for row in cur_trans.fetchall():
            (id_factura, metodo_pago, id_juego, id_cliente, id_proveedor, id_sucursal) = row

            id_estudio = id_proveedor

            # Asegurar que el estudio existe en la analítica
            cur_ana.execute("SELECT 1 FROM estudio WHERE id_estudio = %s", (id_estudio,))
            if not cur_ana.fetchone():
                cur_aux.execute("SELECT nombre FROM empresas WHERE id_empresa = %s", (id_estudio,))
                nombre_estudio = cur_aux.fetchone()[0] if cur_aux.rowcount > 0 else f"Estudio_{id_estudio}"
                cur_ana.execute("INSERT INTO estudio (id_estudio, nombre) VALUES (%s, %s)", (id_estudio, nombre_estudio))

            # Usar el cursor auxiliar para no romper el fetchall del principal
            cur_aux.execute("""
                SELECT genero_id FROM videojuegogenero WHERE videojuego_id = %s
            """, (id_juego,))
            vj = cur_aux.fetchone()
            id_genero = vj[0] if vj else None

            # Obtener los nombres de región y comuna de la sucursal
            cur_aux.execute("""
                SELECT region, comuna FROM sucursales WHERE id_sucursal = %s
            """, (id_sucursal,))
            suc = cur_aux.fetchone()
            region_nombre = suc[0] if suc else None  # region (real)
            comuna_nombre = suc[1] if suc else None  # comuna (real)

            # Buscar el id_region real usando el nombre de la columna comuna de la transaccional
            id_region = None
            if comuna_nombre:
                cur_ana.execute("SELECT id_region FROM region WHERE region_nombre = %s", (comuna_nombre,))
                reg = cur_ana.fetchone()
                id_region = reg[0] if reg else None

            # Buscar el id_comuna real
            id_comuna = None
            if comuna_nombre:
                cur_ana.execute("SELECT id_comuna FROM comuna WHERE comuna_nombre = %s", (comuna_nombre,))
                com = cur_ana.fetchone()
                id_comuna = com[0] if com else None

            # Obtener fecha_compra y precio_clp
            cur_aux.execute("SELECT fecha, monto_factura FROM facturas WHERE id_factura = %s", (id_factura,))
            fact = cur_aux.fetchone()
            fecha_compra = fact[0] if fact else None
            precio_clp = fact[1] if fact else None

            # Obtener fecha_nacimiento y sexo del cliente
            cur_aux.execute("SELECT fecha_nacimiento, sexo FROM clientes WHERE id_cliente = %s", (id_cliente,))
            cli = cur_aux.fetchone()
            fecha_nacimiento = cli[0] if cli else None
            sexo = cli[1] if cli else None

            # Calcular edad y asignar id_rango_etario
            if fecha_nacimiento:
                hoy = date.today()
                edad = hoy.year - fecha_nacimiento.year - ((hoy.month, hoy.day) < (fecha_nacimiento.month, fecha_nacimiento.day))
            else:
                edad = None

            if edad is not None:
                if edad < 10:
                    id_rango_etario = 0
                elif 10 <= edad <= 19:
                    id_rango_etario = 1
                elif 20 <= edad <= 29:
                    id_rango_etario = 2
                elif 30 <= edad <= 39:
                    id_rango_etario = 3
                else:
                    id_rango_etario = 4
            else:
                id_rango_etario = None

            # Asignar id_sexo
            if sexo is not None:
                if sexo.lower() == "hombre":
                    id_sexo = 1
                else:
                    id_sexo = 2
            else:
                id_sexo = None

            # Obtener id_formato, id_multiplayer del videojuego
            cur_aux.execute("SELECT formato, multiplayer FROM videojuegos WHERE id_videojuego = %s", (id_juego,))
            vj2 = cur_aux.fetchone()
            formato = vj2[0] if vj2 else None
            multiplayer_val = vj2[1] if vj2 else None

            # Convertir formato a entero: 1 = Fisico, 2 = Digital
            if formato is not None:
                if formato.lower() == "fisico":
                    id_formato = 1
                elif formato.lower() == "digital":
                    id_formato = 2
                else:
                    id_formato = None
            else:
                id_formato = None

            # Obtener id_multiplayer real desde la tabla multiplayer de análisis
            id_multiplayer = None
            if multiplayer_val is not None:
                cur_ana.execute("SELECT id_multiplayer FROM multiplayer WHERE multiplayer = %s", (multiplayer_val,))
                res = cur_ana.fetchone()
                id_multiplayer = res[0] if res else None

            print(f"""
Insertando hechos_ventas:
    id_compra      = {id_factura}
    id_juego       = {id_juego}
    metodo_pago    = {metodo_pago}
    id_cliente     = {id_cliente}
    id_sucursal    = {id_sucursal}
    id_estudio     = {id_estudio}
    id_genero      = {id_genero}
    id_region      = {id_region}
    id_comuna      = {id_comuna}
    fecha_compra   = {fecha_compra}
    precio_clp     = {precio_clp}
    id_rango_etario= {id_rango_etario}
    id_sexo        = {id_sexo}
    id_formato     = {id_formato}
    id_multiplayer  = {id_multiplayer}
""")
            cur_ana.execute("""
                INSERT INTO hechos_ventas (
                    id_compra, id_juego, fecha_compra, precio_clp, id_cliente, id_sucursal, id_estudio, id_rango_etario, id_sexo, id_formato, id_multiplayer, id_genero, id_region, id_comuna
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                id_factura, id_juego, fecha_compra, precio_clp, id_cliente, id_sucursal, id_estudio, id_rango_etario, id_sexo, id_formato, id_multiplayer, id_genero, id_region, id_comuna
            ))

        conn_ana.commit()
        print("Sincronización completada.")

if __name__ == "__main__":
    while True:
        print("Ejecutando ETL...")
        vaciar_base_analisis()
        sincronizar_y_mostrar()
        print("Esperando 1 minuto para la próxima ejecución...")
        time.sleep(60)
