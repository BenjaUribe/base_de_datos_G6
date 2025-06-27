import psycopg2
from faker import Faker
import random
import datetime
import time
#from datetime import date

# Conectar a la base de datos PostgreSQL
conn = psycopg2.connect(
    dbname="base_datos",  # Cambia esto por el nombre de tu base de datos
    user="postgres",   # Cambia esto por tu nombre de usuario
    password="zawarudo",  # Cambia esto por tu contraseña
    host="localhost",     # Cambia esto si tu base de datos está en otro host
    port="5432"           # Cambia esto si tu base de datos usa un puerto diferente
)
cursor = conn.cursor()

fake = Faker('es_ES')

cant_datos = 15000

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


start_time = time.time()

# Generar y llenar los datos
for _ in range(cant_datos):
    # Generar datos para cliente
    fecha_min = datetime.date(1960, 1, 1)
    fecha_max = datetime.date(2015, 1, 1)

    nombre_cliente = ""
    sexo_cliente = random.choice([1, 0])
    if sexo_cliente == 1:
        nombre_cliente = fake.name_male()
    else:
        nombre_cliente = fake.name_female()

    fecha_nacimiento_cliente = fake.date_between(start_date=fecha_min, end_date=fecha_max)
    
    # insertar sexo si no existe
    cursor.execute('SELECT id_sexo FROM sexo WHERE sexo = %s', (sexo_cliente,))
    result = cursor.fetchone()
    if result:
        id_sexo = result[0]
    else:
        cursor.execute("INSERT INTO sexo (sexo) VALUES (%s) RETURNING id_sexo", (sexo_cliente,))
        id_sexo = cursor.fetchone()[0]

    # Insertar cliente
    cursor.execute("""
        INSERT INTO cliente (nombre, fecha_nacimiento)
        VALUES (%s, %s) RETURNING id_cliente
    """, (nombre_cliente, fecha_nacimiento_cliente))
    id_cliente = cursor.fetchone()[0]

    

    # Generar datos para estudio


    nombre_estudio = random.choice(list(juegos_estudio.keys()))
    nombre_juego = random.choice(juegos_estudio[nombre_estudio])
    
    # Generar datos para  juego
    #nombre_juego = fake.name()
    formato_juego = random.choice(["Fisico", "Digital"])
    genero_juego, modalidad = info_juegos[nombre_juego]
    modalidad_juego = "1" if modalidad == "Si" else "0"

    cursor.execute("""
    SELECT id_estudio FROM estudio WHERE nombre = %s
    """, (nombre_estudio,))
    resultado = cursor.fetchone()

    if resultado:
        id_estudio = resultado[0]  # Ya existe, usar el id existente
    else:
    # No existe, insertarlo
        cursor.execute("""
            INSERT INTO estudio (nombre)
            VALUES (%s) RETURNING id_estudio
            """, (nombre_estudio,))
        id_estudio = cursor.fetchone()[0]

    # Insertar genero si no existe
    cursor.execute("SELECT id_genero FROM genero WHERE nombre_genero = %s", (genero_juego,))
    result = cursor.fetchone()
    if result:
        id_genero = result[0]
    else:
        cursor.execute("INSERT INTO genero (nombre_genero) VALUES (%s) RETURNING id_genero", (genero_juego,))
        id_genero = cursor.fetchone()[0]

    # Formato
    cursor.execute("SELECT id_formato FROM formato WHERE formato = %s", (formato_juego,))
    result = cursor.fetchone()
    if result:
        id_formato = result[0]
    else:
        cursor.execute("INSERT INTO formato (formato) VALUES (%s) RETURNING id_formato", (formato_juego,))
        id_formato = cursor.fetchone()[0]

    # Multiplayer
    cursor.execute("SELECT id_multiplayer FROM multiplayer WHERE multiplayer = %s", (modalidad_juego,))
    result = cursor.fetchone()
    if result:
        id_multiplayer = result[0]
    else:
        cursor.execute("INSERT INTO multiplayer (multiplayer) VALUES (%s) RETURNING id_multiplayer", (modalidad_juego,))
        id_multiplayer = cursor.fetchone()[0]


    # Verificar si el juego ya existe
    cursor.execute("SELECT id_juego FROM juego WHERE nombre = %s", (nombre_juego,))
    result = cursor.fetchone()

    # Juego
    cursor.execute("SELECT id_juego FROM juego WHERE nombre = %s", (nombre_juego,))
    result = cursor.fetchone()
    if result:
        id_juego = result[0]
    else:
        cursor.execute("""
            INSERT INTO juego (nombre)
            VALUES (%s) RETURNING id_juego
        """, (nombre_juego,))
        id_juego = cursor.fetchone()[0]
 

    # Generar datos para sucursal
    nombre_region = random.choice(list(comunas_region.keys()))
    nombre_comuna = random.choice(comunas_region[nombre_region])
    nombre_sucursal = random.choice(sucursal_comuna[nombre_comuna])

    #region
    cursor.execute("SELECT id_region FROM region WHERE region_nombre = %s", (nombre_region,))
    result = cursor.fetchone()
    if result:
        id_region = result[0]
    else:
        cursor.execute("INSERT INTO region (region_nombre) VALUES (%s) RETURNING id_region", (nombre_region,))
        id_region = cursor.fetchone()[0]

    #comuna
    cursor.execute("SELECT id_comuna FROM comuna WHERE comuna_nombre = %s", (nombre_comuna,))
    result = cursor.fetchone()
    if result:
        id_comuna = result[0]
    else:
        cursor.execute("INSERT INTO comuna (comuna_nombre) VALUES (%s) RETURNING id_comuna", (nombre_comuna,))
        id_comuna = cursor.fetchone()[0]

    #sucursal
    nombre_sucursal = random.choice(sucursal_comuna[nombre_comuna])
    cursor.execute("""
        SELECT id_sucursal FROM sucursal WHERE nombre_sucursal = %s
    """, (nombre_sucursal,))
    result = cursor.fetchone()
    if result:
        id_sucursal = result[0]
    else:
        cursor.execute("""
            INSERT INTO sucursal (nombre_sucursal) VALUES (%s) RETURNING id_sucursal
        """, (nombre_sucursal,))
        id_sucursal = cursor.fetchone()[0]

    
    # Verificar si la sucursal ya existe
    
     
    # Generar datos para hechos_ventas
    ##id_boleta = random.randint(1000, 9999)
    fecha_compra = fake.date_this_decade()
    precio = random.randint(5000, 40000)
    ##kw_medidos = random.randint(1, 100)
    ##fecha_medicion = fake.date_this_year()


    # Insertar hechos_ventas
    cursor.execute("""
        INSERT INTO hechos_ventas (
            id_juego, fecha_compra, precio_clp, id_cliente,
            id_sucursal, id_estudio, id_sexo,
            id_formato, id_multiplayer, id_genero,
            id_region, id_comuna
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        id_juego, fecha_compra, precio, id_cliente,
        id_sucursal, id_estudio, id_sexo,
        id_formato, id_multiplayer, id_genero,
        id_region, id_comuna
    ))

# Confirmar los cambios en la base de datos
conn.commit()

# Cerrar la conexión
cursor.close()
conn.close()

end_time = time.time()
elapsed_time = end_time - start_time
print("Se han insertado ", cant_datos, " datos aleatorios.")
print(f"Tiempo de ejecución: {elapsed_time:.4f} segundos")