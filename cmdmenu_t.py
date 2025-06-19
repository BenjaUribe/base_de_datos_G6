import psycopg2
import subprocess
import os

DB_CONFIG = {
    "dbname": "venta_juegos_t",
    "user": "admin",
    "password": "Admin132",
    "host": "localhost",
    "port": "5432"
}

def conectar():
    return psycopg2.connect(**DB_CONFIG)

# CRUD CLIENTES
def ingresar_cliente():
    nombre = input("Nombre: ")
    sexo = input("Sexo (Hombre/Mujer): ")
    fecha_nacimiento = input("Fecha de nacimiento (YYYY-MM-DD): ")
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO clientes (nombre, sexo, fecha_nacimiento) VALUES (%s, %s, %s)",
                (nombre, sexo, fecha_nacimiento)
            )
    print("Cliente ingresado.")

def modificar_cliente():
    id_cliente = input("ID del cliente a modificar: ")
    nombre = input("Nuevo nombre: ")
    sexo = input("Nuevo sexo: ")
    fecha_nacimiento = input("Nueva fecha de nacimiento (YYYY-MM-DD): ")
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE clientes SET nombre=%s, sexo=%s, fecha_nacimiento=%s WHERE id_cliente=%s",
                (nombre, sexo, fecha_nacimiento, id_cliente)
            )
    print("Cliente modificado.")

def listar_clientes():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id_cliente, nombre, sexo, fecha_nacimiento FROM clientes ORDER BY id_cliente")
            print("\nClientes:")
            for row in cur.fetchall():
                print(row)

def eliminar_cliente():
    id_cliente = input("ID del cliente a eliminar: ")
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM clientes WHERE id_cliente=%s", (id_cliente,))
    print("Cliente eliminado.")

# CRUD VIDEOJUEGOS
def ingresar_videojuego():
    nombre = input("Nombre del videojuego: ")
    formato = input("Formato (Fisico/Digital): ")
    multiplayer = input("¿Multiplayer? (1=Sí, 0=No): ")
    valor = input("Valor: ")
    empresa_id = input("ID de la empresa: ")
    proveedor_id = input("ID del proveedor: ")
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO videojuegos (nombre, formato, multiplayer, valor, empresa_id, proveedor_id) VALUES (%s, %s, %s, %s, %s, %s)",
                (nombre, formato, multiplayer, valor, empresa_id, proveedor_id)
            )
    print("Videojuego ingresado.")

def modificar_videojuego():
    id_videojuego = input("ID del videojuego a modificar: ")
    nombre = input("Nuevo nombre: ")
    formato = input("Nuevo formato: ")
    multiplayer = input("¿Multiplayer? (1=Sí, 0=No): ")
    valor = input("Nuevo valor: ")
    empresa_id = input("Nuevo ID de empresa: ")
    proveedor_id = input("Nuevo ID de proveedor: ")
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE videojuegos SET nombre=%s, formato=%s, multiplayer=%s, valor=%s, empresa_id=%s, proveedor_id=%s WHERE id_videojuego=%s",
                (nombre, formato, multiplayer, valor, empresa_id, proveedor_id, id_videojuego)
            )
    print("Videojuego modificado.")

def listar_videojuegos():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id_videojuego, nombre, formato, multiplayer, valor, empresa_id, proveedor_id FROM videojuegos ORDER BY id_videojuego")
            print("\nVideojuegos:")
            for row in cur.fetchall():
                print(row)

def eliminar_videojuego():
    id_videojuego = input("ID del videojuego a eliminar: ")
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM videojuegos WHERE id_videojuego=%s", (id_videojuego,))
    print("Videojuego eliminado.")

def vaciar_base():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            TRUNCATE clientes, empresas, facturas, generos, ofertas, proveedor, sucursales, videojuegos, detallefactura, empleados, vendedores, proveedorsucursal, stock, videojuegogenero RESTART IDENTITY CASCADE;
        """)
    conn.commit()
    print("Base de datos vaciada.")

def ejecutar_insercion():
    vaciar_base()
    print("Ejecutando inserción masiva...")
    ruta = os.path.join(os.path.dirname(__file__), "insercion_datos_transaccional.py")
    subprocess.run(["python", ruta], check=True)
    print("Inserción masiva finalizada.")

def menu():
    while True:
        print("""
1. Ingresar cliente
2. Modificar cliente
3. Listar clientes
4. Eliminar cliente
5. Ingresar videojuego
6. Modificar videojuego
7. Listar videojuegos
8. Eliminar videojuego
9. Cargar datos de prueba
10. Salir
""")
        opcion = input("Seleccione una opción: ")
        if opcion == '1':
            ingresar_cliente()
        elif opcion == '2':
            modificar_cliente()
        elif opcion == '3':
            listar_clientes()
        elif opcion == '4':
            eliminar_cliente()
        elif opcion == '5':
            ingresar_videojuego()
        elif opcion == '6':
            modificar_videojuego()
        elif opcion == '7':
            listar_videojuegos()
        elif opcion == '8':
            eliminar_videojuego()
        elif opcion == '9':
            ejecutar_insercion()
        elif opcion == '10':
            print("¡Hasta luego!")
            break
        else:
            print("Opción no válida.")

if __name__ == "__main__":
    menu()