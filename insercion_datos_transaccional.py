import psycopg2
from faker import Faker
import random
import datetime
import time

conn = psycopg2.connect(
    dbname="venta_juegos_t",
    user="admin",
    password="Admin132",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()
fake = Faker('es_ES')

cant_datos = 50000

#agregar los diccionarios de comuna_region, sucursal_comuna, juegos_estuidiom, info_juegos
comunas_region = {"Región de Arica y Parinacota" : ["Arica"],
                    "Región de Tarapacá": ["Iquique"],
                    "Región de Antofagasta": ["Antofagasta", "Calama"],
                    "Región de Atacama": ["Copiapó"],
                    "Región de Coquimbo": ["Coquimbo", "La Serena", "Ovalle"],
                    "Región de Valparaíso": ["Valparaíso", "Viña del Mar", "Quilpué", "Quillota"],
                    "Región Metropolitana de Santiago": ["Estación Central", "Maipú", "Las Condes", "La Florida", "Puente Alto", "La Reina", "Providencia", "Cerrillos", "San Bernardo"],
                    "Región de O'Higgins": ["Rancagua", "San Fernando"],
                    "Región del Maule": ["Talca", "Curicó", "Linares"],
                    "Región del Ñuble": ["Chillán"],
                    "Región del Biobío": ["Concepción", "Talcahuano", "Los Ángeles", "Coronel", "San Pedro de La Paz"], 
                    "Región de La Araucanía": ["Temuco", "Pucón"],
                    "Región de Los Ríos": ["Valdivia"],
                    "Región de Los Lagos": ["Puerto Montt", "Osorno", "Los Muermos", "Puerto Varas", "Castro"],
                    "Región de Aysén": ["Coyhaique"],
                    "Región de Magallanes y de la Antártica Chilena": ["Punta Arenas"]}

sucursal_comuna = {"Arica": ["Mallplaza Arica"], "Iquique": ["Mallplaza Iquique"], "Antofagasta": ["Mallplaza Antofagasta", "Paseo La Portada"], "Calama": ["Mallplaza Calama"], 
                    "Copiapó": ["Mallplaza Copiapó"], "Coquimbo": ["Mall Vivo Coquimbo"], "La Serena": ["Mallplaza La Serena", "Puerta del Mar"], "Ovalle": ["Open Plaza Ovalle"],
                    "Valparaíso": ["Portal Valparaiso", "Paseo Ross"], "Viña del Mar": ["Mall Marina", "Paseo Viña Centro"], "Quillota": ["Paseo Shopping Quillota"],
                    "Quilpué": ["Paseo Quilpué"], "Estación Central": ["Mallplaza Alameda"], "La Reina": ["Mallplaza Egaña", "Portal La Reina"], "La Florida": ["Mallplaza Vespucio"],
                    "Las Condes": ["Parque Arauco", "Alto Las Condes", "Mallplaza Los Dominicos"], "Providencia": ["Costanera Center"], "Puente Alto": ["Mallplaza Tobalaba"],
                    "Cerrillos": ["Mallplaza Oeste"], "Maipú": ["Mall Arauco Maipú"], "San Bernardo": ["Mallplaza Sur"], "Rancagua": ["Patio Rancagua", "Open Plaza Rancagua"],
                    "San Fernando": ["Mall Vivo San Fernando"], "Talca": ["Plaza Maule"], "Curicó": ["Mall Curicó"], "Linares": ["Espacio Urbano Linares"], 
                    "Chillán": ["Open Chillán", "Mall Arauco Chillán"], "Concepción": ["Mall del Centro"], "Los Ángeles": ["Mallplaza Los Ángeles"], "San Pedro de La Paz": ["Paseo San Pedro"],
                    "Talcahuano": ["Mallplaza Trébol"], "Coronel": ["Mall Arauco Coronel"], "Temuco": ["Portal Temuco", "Patio Outlet Temuco"], "Pucón": ["Centro Comercial Los Álamos"],
                    "Valdivia": ["Plaza de Los Ríos", "Portal Valdivia"], "Puerto Montt": ["Paseo Costanera", "Paseo del Mar"], "Osorno": ["Portal Osorno"], "Puerto Varas": ["Paseo Puerto Varas"],
                    "Los Muermos": ["Centro Comercial Patio Varas"], "Castro": ["Paseo Chiloé"], "Coyhaique": ["Paseo Peatonal Horn"], "Punta Arenas": ["Espacio Urbano Pionero"]}

juegos_estudio = {"SEGA": ["Sonic", "Yakuza", "Persona"],
                  "Mojang": ["Minecraft"], 
                  "CAPCOM": ["Street Fighter", "Resident Evil", "Monster Hunter"],
                  "Activision": ["Call of Duty", "Tony Hawk's Pro Skater", "Crash Bandicoot"],
                  "Bethesda": ["Fallout", "The Elder Scrolls", "Doom", "Wolfenstein"],
                  "EA": ["FIFA", "Battlefield", "The Sims", "Star Wars: Battlefront", "Dead Space", "Mass Effect", "UFC", "Need for Speed"],
                  "Rockstar": ["GTA SA", "Red Dead Redemption", "Max Payne", "Bully", "Midnight Club", "GTA V", "GTA IV", "GTA III", "GTA: Vice City"],
                  "Ubisoft": ["Assassin's Creed", "Far Cry", "Watch Dogs"],
                  "Bandai Namco": ["Tekken", "Dragon Ball Z"],
                  "Valve": ["Half-Life", "Portal", "Dota 2", "Counter-Strike", "Left 4 Dead", "Portal 2"],
                  "Epic Games": ["Fortnite", "Unreal Tournament", "Gears of War"],
                  "Naughty Dog": ["Uncharted", "The Last of Us", "Jak and Daxter"],
                  "Respawn Entertainment": ["Titanfall", "Apex Legends"],
                  "Bungie": ["Halo", "Destiny"],
                  "Treyarch": ["Call of Duty: Black Ops"],
                  "Infinity War": ["Call of Duty: Modern Warfare", "Call of Duty: Ghosts", "Call of Duty: Infinite Warfare", "Call of Duty 2"],
                  "FromSoftware": ["Dark Souls", "Bloodborne", "Sekiro"],
                  "Nintendo": ["Super Mario", "The Legend of Zelda", "Metroid", "Pokémon", "Mario Kart", "Splatoon", "Animal Crossing", "Fire Emblem", "Kirby", "Donkey Kong"],}

info_juegos = {
    "Sonic": ("Plataformas", "No"),
    "Yakuza": ("Acción/Aventura", "No"),
    "Persona": ("JRPG", "No"),
    "Minecraft": ("Sandbox", "Si"),
    "Street Fighter": ("Lucha", "Si"),
    "Resident Evil": ("Survival Horror", "No"),
    "Monster Hunter": ("Acción/RPG", "Si"),
    "Call of Duty": ("Shooter", "Si"),
    "Tony Hawk's Pro Skater": ("Deportes", "No"),
    "Crash Bandicoot": ("Plataformas", "No"),
    "Fallout": ("RPG", "No"),
    "The Elder Scrolls": ("RPG", "No"),
    "Doom": ("Shooter", "No"),
    "Wolfenstein": ("Shooter", "No"),
    "FIFA": ("Deportes", "Si"),
    "Battlefield": ("Shooter", "Si"),
    "The Sims": ("Simulación", "No"),
    "Star Wars: Battlefront": ("Shooter", "Si"),
    "Dead Space": ("Survival Horror", "No"),
    "Mass Effect": ("RPG", "No"),
    "UFC": ("Deportes", "Si"),
    "Need for Speed": ("Carreras", "Si"),
    "GTA SA": ("Acción/Aventura", "No"),
    "Red Dead Redemption": ("Acción/Aventura", "No"),
    "Max Payne": ("Shooter", "No"),
    "Bully": ("Acción/Aventura", "No"),
    "Midnight Club": ("Carreras", "Si"),
    "GTA V": ("Acción/Aventura", "Si"),
    "GTA IV": ("Acción/Aventura", "Si"),
    "GTA III": ("Acción/Aventura", "No"),
    "GTA: Vice City": ("Acción/Aventura", "No"),
    "Assassin's Creed": ("Acción/Aventura", "No"),
    "Far Cry": ("Shooter", "No"),
    "Watch Dogs": ("Acción/Aventura", "No"),
    "Tekken": ("Lucha", "Si"),
    "Dragon Ball Z": ("Lucha", "Si"),
    "Half-Life": ("Shooter", "No"),
    "Portal": ("Puzzle", "No"),
    "Dota 2": ("MOBA", "Si"),
    "Counter-Strike": ("Shooter", "Si"),
    "Left 4 Dead": ("Shooter", "Si"),
    "Portal 2": ("Puzzle", "Si"),
    "Fortnite": ("Battle Royale", "Si"),
    "Unreal Tournament": ("Shooter", "Si"),
    "Gears of War": ("Shooter", "Si"),
    "Uncharted": ("Acción/Aventura", "No"),
    "The Last of Us": ("Acción/Aventura", "No"),
    "Jak and Daxter": ("Plataformas", "No"),
    "Titanfall": ("Shooter", "Si"),
    "Apex Legends": ("Battle Royale", "Si"),
    "Halo": ("Shooter", "Si"),
    "Destiny": ("Shooter", "Si"),
    "Call of Duty: Black Ops": ("Shooter", "Si"),
    "Call of Duty: Modern Warfare": ("Shooter", "Si"),
    "Call of Duty: Ghosts": ("Shooter", "Si"),
    "Call of Duty: Infinite Warfare": ("Shooter", "Si"),
    "Call of Duty 2": ("Shooter", "Si"),
    "Dark Souls": ("Acción/RPG", "No"),
    "Bloodborne": ("Acción/RPG", "No"),
    "Sekiro": ("Acción/Aventura", "No"),
    "Super Mario": ("Plataformas", "No"),
    "The Legend of Zelda": ("Aventura", "No"),
    "Metroid": ("Acción/Aventura", "No"),
    "Pokémon": ("JRPG", "No"),
    "Mario Kart": ("Carreras", "Si"),
    "Splatoon": ("Shooter", "Si"),
    "Animal Crossing": ("Simulación", "Si"),
    "Fire Emblem": ("Estrategia", "No"),
    "Kirby": ("Plataformas", "No"),
    "Donkey Kong": ("Plataformas", "No"),
}

dict_ofertas = {'0': [0.2, "10-12-2000", "10-01-2025"], '1': [0.2, "15-03-2000", "15-04-2025"], '2': [0.18, "15-09-2000", "20-09-2025"], '3': [0.2, "15-10-2000", "31-10-2025"]}

empleados =    {
    "Mallplaza Arica": 
        {"nombre": "Ana Pérez", "RUT": "17.345.678-9", "sueldo": 510000},
    "Mallplaza Iquique": 
        {"nombre": "Carla Soto", "RUT": "15.987.654-3", "sueldo": 870000},
    "Mallplaza Antofagasta": 
        {"nombre": "María López", "RUT": "16.543.210-1", "sueldo": 950000},
    "Paseo La Portada":
        {"nombre": "Pedro Fernández", "RUT": "17.654.321-0", "sueldo": 890000},
    "Mallplaza Calama": 
        {"nombre": "Luis González", "RUT": "18.765.432-2", "sueldo": 920000},
    "Mallplaza Copiapó": 
        {"nombre": "Ana Torres", "RUT": "19.876.543-1", "sueldo": 930000},
    "Mall Vivo Coquimbo": 
        {"nombre": "Roberto Martínez", "RUT": "20.987.654-2", "sueldo": 940000},
    "Mallplaza La Serena": 
        {"nombre": "Claudia Ramírez", "RUT": "21.098.765-3", "sueldo": 960000},
    "Puerta del Mar": 
        {"nombre": "Sofía Morales", "RUT": "22.109.876-4", "sueldo": 980000},
    "Open Plaza Ovalle": 
        {"nombre": "Diego Herrera", "RUT": "21.210.987-5", "sueldo": 1000000},
    "Portal Valparaiso": 
        {"nombre": "Laura Castillo", "RUT": "15.321.098-6", "sueldo": 1020000},
    "Paseo Ross": 
        {"nombre": "Javier Díaz", "RUT": "22.432.109-7", "sueldo": 1040000},
    "Mall Marina": 
        {"nombre": "Gabriela Silva", "RUT": "16.543.210-8", "sueldo": 1060000},
    "Paseo Viña Centro": 
        {"nombre": "Andrés Torres", "RUT": "17.654.321-9", "sueldo": 1080000},
    "Paseo Shopping Quillota": 
        {"nombre": "Patricia Gómez", "RUT": "18.765.432-0", "sueldo": 1100000},
    "Paseo Quilpué": 
        {"nombre": "Fernando Ruiz", "RUT": "19.876.543-1", "sueldo": 1120000},
    "Mallplaza Alameda": 
        {"nombre": "Isabel Castro", "RUT": "20.987.654-2", "sueldo": 1140000},
    "Mallplaza Egaña": 
        {"nombre": "Carlos Jiménez", "RUT": "21.098.765-3", "sueldo": 1160000},
    "Portal La Reina": 
        {"nombre": "Verónica Pérez", "RUT": "22.109.876-4", "sueldo": 1180000},
    "Mallplaza Vespucio": 
        {"nombre": "Eduardo Sánchez", "RUT": "16.210.987-5", "sueldo": 1200000},
    "Parque Arauco": 
        {"nombre": "Natalia Torres", "RUT": "19.321.098-6", "sueldo": 1220000},
    "Mallplaza Los Dominicos": 
        {"nombre": "Ricardo Morales", "RUT": "15.432.109-7", "sueldo": 1240000},
    "Alto Las Condes": 
        {"nombre": "Lucía Fernández", "RUT": "16.543.210-8", "sueldo": 1260000},
    "Costanera Center": 
        {"nombre": "Héctor González", "RUT": "17.654.321-9", "sueldo": 1280000},
    "Mallplaza Tobalaba": 
        {"nombre": "Marta Ramírez", "RUT": "18.765.432-0", "sueldo": 1300000},
    "Mallplaza Oeste": 
        {"nombre": "Álvaro Herrera", "RUT": "19.876.543-1", "sueldo": 1320000},
    "Mall Arauco Maipú": 
        {"nombre": "Clara Castillo", "RUT": "20.987.654-2", "sueldo": 1340000},
    "Mallplaza Sur": 
        {"nombre": "Sergio Díaz", "RUT": "21.098.765-3", "sueldo": 1360000},
    "Patio Rancagua": 
        {"nombre": "Valentina Castro", "RUT": "22.109.876-4", "sueldo": 1380000},
    "Open Plaza Rancagua": 
        {"nombre": "Jorge Jiménez", "RUT": "16.210.987-5", "sueldo": 1400000},
    "Mall Vivo San Fernando": 
        {"nombre": "Camila Pérez", "RUT": "18.321.098-6", "sueldo": 1420000},
    "Plaza Maule": 
        {"nombre": "Gonzalo Sánchez", "RUT": "15.432.109-7", "sueldo": 1440000},
    "Mall Curicó": 
        {"nombre": "Paula Torres", "RUT": "16.543.210-8", "sueldo": 1460000},
    "Espacio Urbano Linares": 
        {"nombre": "Diego López", "RUT": "17.654.321-9", "sueldo": 1480000},
    "Open Chillán": 
        {"nombre": "Elena González", "RUT": "18.765.432-0", "sueldo": 1500000},
    "Mall Arauco Chillán": 
        {"nombre": "Roberto Ramírez", "RUT": "19.876.543-1", "sueldo": 1520000},
    "Mall del Centro": 
        {"nombre": "Sofía Herrera", "RUT": "20.987.654-2", "sueldo": 1540000},
    "Mallplaza Los Ángeles": 
        {"nombre": "Fernando Castillo", "RUT": "21.098.765-3", "sueldo": 1560000},
    "Paseo San Pedro": 
        {"nombre": "Claudia Díaz", "RUT": "22.109.876-4", "sueldo": 1580000},
    "Mallplaza Trébol": 
        {"nombre": "Andrés Castro", "RUT": "19.210.987-5", "sueldo": 1600000},
    "Mall Arauco Coronel": 
        {"nombre": "Patricia Jiménez", "RUT": "21.321.098-6", "sueldo": 1620000},
    "Portal Temuco": 
        {"nombre": "Gabriel Pérez", "RUT": "15.432.109-7", "sueldo": 1640000},
    "Patio Outlet Temuco": 
        {"nombre": "Isabel Sánchez", "RUT": "16.543.210-8", "sueldo": 1660000},
    "Centro Comercial Los Álamos": 
        {"nombre": "Carlos Torres", "RUT": "17.654.321-9", "sueldo": 1680000},
    "Plaza de Los Ríos": 
        {"nombre": "Verónica López", "RUT": "18.765.432-0", "sueldo": 1700000},
    "Portal Valdivia": 
        {"nombre": "Eduardo González", "RUT": "19.876.543-1", "sueldo": 1720000},
    "Paseo Costanera": 
        {"nombre": "Natalia Ramírez", "RUT": "20.987.654-2", "sueldo": 1740000},
    "Paseo del Mar": 
        {"nombre": "Ricardo Herrera", "RUT": "21.098.765-3", "sueldo": 1760000},
    "Portal Osorno": 
        {"nombre": "Lucía Castillo", "RUT": "22.109.876-4", "sueldo": 1780000},
    "Paseo Puerto Varas": 
        {"nombre": "Héctor Díaz", "RUT": "22.210.987-5", "sueldo": 1800000},
    "Centro Comercial Patio Varas": 
        {"nombre": "Marta Castro", "RUT": "21.321.098-6", "sueldo": 1820000},
    "Paseo Chiloé": 
        {"nombre": "Álvaro Jiménez", "RUT": "20.432.109-7", "sueldo": 1840000},
    "Paseo Peatonal Horn": 
        {"nombre": "Clara Pérez", "RUT": "16.543.210-8", "sueldo": 1860000},
    "Espacio Urbano Pionero": 
        {"nombre": "Sergio González", "RUT": "17.654.321-9", "sueldo": 1880000},
}


start_time = time.time()
for _ in range(cant_datos):
    fecha_min = datetime.date(1960, 1, 1)
    fecha_max = datetime.date(2015, 1, 1)
    fecha_nacimiento = fake.date_between(start_date=fecha_min, end_date=fecha_max)

    nombre_cliente = ""
    sexo_cliente = random.choice(["Hombre", "Mujer"])
    if sexo_cliente == "Hombre":
        nombre_cliente = fake.name_male()
    else:
        nombre_cliente = fake.name_female()
    
    #insertar cliente
    cursor.execute("""
        INSERT INTO clientes (nombre, sexo, fecha_nacimiento)
        VALUES (%s, %s, %s) RETURNING id_cliente
    """, (nombre_cliente, sexo_cliente, fecha_nacimiento))
    id_cliente = cursor.fetchone()[0] 

    #insertar sucursal
    nombre_region = random.choice(list(comunas_region.keys()))
    nombre_comuna = random.choice(comunas_region[nombre_region])
    nombre_sucursal = random.choice(sucursal_comuna[nombre_comuna])

    cursor.execute("""
        SELECT id_sucursal FROM sucursales WHERE nombre = %s
    """, (nombre_sucursal,))
    result = cursor.fetchone()
    if result:
        id_sucursal = result[0]
    else:
        cursor.execute("""
            INSERT INTO sucursales (nombre, region, comuna)
            VALUES (%s, %s, %s) RETURNING id_sucursal
        """, (nombre_sucursal, nombre_comuna, nombre_region))
        id_sucursal = cursor.fetchone()[0]


    #insertar proveedor
    monto_total = random.randint(5000, 40000)
    
    cursor.execute("""
        INSERT INTO proveedor (precios)
        VALUES (%s) RETURNING id_proveedor
    """, (int(monto_total * 0.5),))
    id_proveedor = cursor.fetchone()[0]

    #insertar proveedorsucursal
    cursor.execute("""
        INSERT INTO proveedorsucursal (proveedor_id, sucursal_id)
        VALUES (%s, %s) 
    """, (id_proveedor, id_sucursal))


    #insertar empresas
    nombre_empresa = random.choice(list(juegos_estudio.keys()))
    nombre_juego = random.choice(juegos_estudio[nombre_empresa])

    cursor.execute("""
    SELECT id_empresa FROM empresas WHERE nombre = %s
    """, (nombre_empresa,)) 
    resultado = cursor.fetchone()

    if resultado:
        id_empresa = resultado[0]  # Ya existe, usar el id existente
    else:
    # No existe, insertarlo
        cursor.execute("""
            INSERT INTO empresas (nombre)
            VALUES (%s) RETURNING id_empresa
            """, (nombre_empresa,))
        id_empresa = cursor.fetchone()[0]



    #insertar generos
    formato_juego = random.choice(["Fisico", "Digital"])
    genero_juego, modalidad = info_juegos[nombre_juego]
    modalidad_juego = "1" if modalidad == "Si" else "0"

    cursor.execute("SELECT id_genero FROM generos WHERE nombre = %s", (genero_juego,))
    result = cursor.fetchone()
    if result:
        id_genero = result[0]
    else:
        cursor.execute("INSERT INTO generos (nombre) VALUES (%s) RETURNING id_genero", (genero_juego,))
        id_genero = cursor.fetchone()[0]

    
    #insertar videojuegos

    cursor.execute("""
        INSERT INTO videojuegos (nombre, formato, multiplayer, valor, empresa_id, proveedor_id)
        VALUES (%s, %s, %s, %s, %s, %s) RETURNING id_videojuego
    """, (nombre_juego, formato_juego, modalidad_juego, monto_total, id_empresa, id_proveedor))
    id_videojuego = cursor.fetchone()[0]

    #insertar videojuegogenero
    cursor.execute("""
        INSERT INTO videojuegogenero (videojuego_id, genero_id)
        VALUES (%s, %s)
    """, (id_videojuego, id_genero))

    #insertar stock
    cantidad_stock = random.randint(1, 20)
    cursor.execute("""
        INSERT INTO stock (cantidad, videojuego_id, sucursal_id)
        VALUES (%s, %s, %s)
    """, (cantidad_stock, id_videojuego, id_sucursal))

    
    empleado_sel = empleados.get(nombre_sucursal)
    nombre_empleado = empleado_sel.get("nombre")
    rut_empleado = empleado_sel.get("RUT")
    sueldo_empleado = empleado_sel.get("sueldo")

    fecha_venta = fake.date_this_decade()

    #insertar empleado
    cursor.execute("""
    INSERT INTO empleados (rut, nombre, sueldo, sucursal_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (rut) DO NOTHING
    """, (rut_empleado, nombre_empleado, sueldo_empleado, id_sucursal))

    #insertar vendedor
    cursor.execute("""
        INSERT INTO empleados (rut) VALUES (%s)
        ON CONFLICT (rut) DO NOTHING
    """, (rut_empleado,))

    # Ahora puedes insertar en vendedores
    cursor.execute("""
        INSERT INTO vendedores (rut_empleado) VALUES (%s)
        ON CONFLICT (rut_empleado) DO NOTHING
    """, (rut_empleado,))

    #insertar factura
    metodo_pago = random.choice(["Efectivo", "Debito", "Credito"])

    cursor.execute("""
        INSERT INTO facturas (fecha, monto_factura, cliente_id, vendedor_rut, metodo_pago)
        VALUES (%s, %s, %s, %s, %s) RETURNING id_factura
    """, (fecha_venta, monto_total, id_cliente, rut_empleado, metodo_pago,))
    id_factura = cursor.fetchone()[0]

    #detallefactura
    cursor.execute("""
        INSERT INTO detallefactura (factura_id, videojuego_id)
        VALUES (%s, %s)
    """, (id_factura, id_videojuego))
    

    #insertar ofertas 
    oferta = random.choice(list(dict_ofertas.keys()))
    porcentaje = dict_ofertas.get(oferta)[0]
    fecha_ini = dict_ofertas.get(oferta)[1]
    fecha_fin = dict_ofertas.get(oferta)[2]
    cursor.execute("""
        INSERT INTO ofertas (porcentaje, fecha_inicio, fecha_termino, videojuego_id)
        VALUES (%s, %s, %s, %s)
    """, (porcentaje, fecha_ini, fecha_fin, id_videojuego))



#cerrar conexion y guardar los cambios
conn.commit()
cursor.close()
conn.close()

end_time = time.time()
elapsed_time = end_time - start_time
print("Se han insertado ", cant_datos, " datos aleatorios.")
print(f"Tiempo de ejecución: {elapsed_time:.4f} segundos")