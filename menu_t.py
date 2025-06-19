import tkinter as tk
from tkinter import messagebox, scrolledtext
import psycopg2
import threading

DB_CONFIG = {
    "dbname": "venta_juegos_t",
    "user": "admin",
    "password": "Admin132",
    "host": "localhost",
    "port": "5432"
}

def conectar():
    return psycopg2.connect(**DB_CONFIG)

def validar_anio(anio):
    if not (anio.isdigit() and len(anio) == 4):
        messagebox.showerror("Error", "El año debe tener 4 dígitos (ej: 2024).")
        return False
    return True

def ingresar_cliente(nombre, sexo, fecha_nacimiento):
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO clientes (nombre, sexo, fecha_nacimiento) VALUES (%s, %s, %s)",
                    (nombre, sexo, fecha_nacimiento)
                )
        messagebox.showinfo("Éxito", "Cliente ingresado.")
    except Exception as e:
        messagebox.showerror("Error", str(e))

def modificar_cliente(id_cliente, nombre, sexo, fecha_nacimiento):
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE clientes SET nombre=%s, sexo=%s, fecha_nacimiento=%s WHERE id_cliente=%s",
                    (nombre, sexo, fecha_nacimiento, id_cliente)
                )
        messagebox.showinfo("Éxito", "Cliente modificado.")
    except Exception as e:
        messagebox.showerror("Error", str(e))

def eliminar_cliente(id_cliente):
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM clientes WHERE id_cliente=%s", (id_cliente,))
        messagebox.showinfo("Éxito", "Cliente eliminado.")
    except Exception as e:
        messagebox.showerror("Error", str(e))

def ingresar_videojuego(nombre, formato, multiplayer, valor, empresa_id, proveedor_id):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO videojuegos (nombre, formato, multiplayer, valor, empresa_id, proveedor_id) VALUES (%s, %s, %s, %s, %s, %s)",
                (nombre, formato, multiplayer, valor, empresa_id, proveedor_id)
            )
    messagebox.showinfo("Éxito", "Videojuego ingresado.")

def modificar_videojuego(id_videojuego, nombre, formato, multiplayer, valor, empresa_id, proveedor_id):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE videojuegos SET nombre=%s, formato=%s, multiplayer=%s, valor=%s, empresa_id=%s, proveedor_id=%s WHERE id_videojuego=%s",
                (nombre, formato, multiplayer, valor, empresa_id, proveedor_id, id_videojuego)
            )
    messagebox.showinfo("Éxito", "Videojuego modificado.")

def listar_videojuegos():
    videojuegos = []
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id_videojuego, nombre, formato, multiplayer, valor, empresa_id, proveedor_id FROM videojuegos ORDER BY id_videojuego")
            videojuegos = cur.fetchall()
    return videojuegos

def eliminar_videojuego(id_videojuego):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM videojuegos WHERE id_videojuego=%s", (id_videojuego,))
    messagebox.showinfo("Éxito", "Videojuego eliminado.")

# --- TKINTER ---

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Menú Tienda BD")
        self.geometry("800x600")
        self.minsize(600, 400)
        self.protocol("WM_DELETE_WINDOW", self.bloquear_cerrar)
        self.menu_screen()

    def bloquear_cerrar(self):
        # No hace nada, así se bloquea el botón X de la ventana
        pass

    def clear(self):
        for widget in self.winfo_children():
            widget.destroy()

    def menu_screen(self):
        self.clear()
        tk.Label(self, text="Menú principal", font=("Arial", 16)).pack(pady=10)
        opciones = (
            "1. Ingresar cliente\n"
            "2. Modificar cliente\n"
            "3. Listar clientes\n"
            "4. Eliminar cliente\n"
            "5. Ingresar videojuego\n"
            "6. Modificar videojuego\n"
            "7. Listar videojuegos\n"
            "8. Eliminar videojuego\n"
            "9. Listar empresas\n"
            "10. Listar proveedores\n"
            "11. Cargar datos de prueba\n"
            "12. Salir"
        )
        tk.Label(self, text=opciones, justify="left").pack()
        self.opcion_entry = tk.Entry(self)
        self.opcion_entry.pack(pady=5)
        tk.Button(self, text="Seleccionar", command=self.seleccionar_opcion).pack()

    def seleccionar_opcion(self):
        opcion = self.opcion_entry.get()
        if opcion == '1':
            self.pantalla_ingresar_cliente()
        elif opcion == '2':
            self.pantalla_modificar_cliente()
        elif opcion == '3':
            self.pantalla_listar_clientes()
        elif opcion == '4':
            self.pantalla_eliminar_cliente()
        elif opcion == '5':
            self.pantalla_ingresar_videojuego()
        elif opcion == '6':
            self.pantalla_modificar_videojuego()
        elif opcion == '7':
            self.pantalla_listar_videojuegos()
        elif opcion == '8':
            self.pantalla_eliminar_videojuego()
        elif opcion == '9':
            self.pantalla_listar_empresas()
        elif opcion == '10':
            self.pantalla_listar_proveedores()
        elif opcion == '11':
            self.pantalla_cargando_datos_prueba()
        elif opcion == '12':
            self.quit()
        else:
            messagebox.showerror("Error", "Opción no válida o no implementada en este ejemplo.")

    def pantalla_cargando_insercion(self):
        self.clear()
        self.cargando_label = tk.Label(self, text="Cargando...", font=("Arial", 18))
        self.cargando_label.pack(pady=30)
        self.tiempo_espera = 0
        self.tiempo_label = tk.Label(self, text="Tiempo transcurrido: 0 s", font=("Arial", 14))
        self.tiempo_label.pack(pady=10)
        self._cronometro_activo = True
        self._actualizar_cronometro()
        self.update()
        if hasattr(self, '_insercion_en_progreso') and self._insercion_en_progreso:
            return
        self._insercion_en_progreso = True
        threading.Thread(target=self.ejecutar_insercion_masiva_safe, daemon=True).start()

    def _actualizar_cronometro(self):
        if getattr(self, '_cronometro_activo', False):
            self.tiempo_espera += 1
            self.tiempo_label.config(text=f"Tiempo transcurrido: {self.tiempo_espera} s")
            self.after(1000, self._actualizar_cronometro)

    def ejecutar_insercion_masiva_safe(self):
        try:
            self.ejecutar_insercion_masiva()
        except Exception as e:
            self.after(0, lambda: messagebox.showerror("Error", str(e)))
        self._insercion_en_progreso = False
        self._cronometro_activo = False
        self.after(0, self.menu_screen)

    def ejecutar_insercion_masiva(self):
        # Aquí va tu lógica real de inserción masiva
        import time
        time.sleep(3)  # Simula proceso largo
        self.after(0, lambda: messagebox.showinfo("Info", "Inserción masiva ejecutada."))

    def pantalla_ingresar_cliente(self):
        self.clear()
        tk.Label(self, text="Ingresar Cliente", font=("Arial", 14)).pack(pady=10)
        form_frame = tk.Frame(self)
        form_frame.pack(pady=10)
        tk.Label(form_frame, text="Nombre:").grid(row=0, column=0, sticky="e", padx=5, pady=5)
        nombre = tk.Entry(form_frame)
        nombre.grid(row=0, column=1, padx=5, pady=5)
        tk.Label(form_frame, text="Sexo:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
        sexo_var = tk.StringVar(value="Hombre")
        sexo_box = tk.OptionMenu(form_frame, sexo_var, "Hombre", "Mujer", "Otros")
        sexo_box.grid(row=1, column=1, padx=5, pady=5)
        tk.Label(form_frame, text="Fecha nacimiento (YYYY-MM-DD):").grid(row=2, column=0, sticky="e", padx=5, pady=5)
        fecha = tk.Entry(form_frame)
        fecha.grid(row=2, column=1, padx=5, pady=5)
        def ejecutar():
            if not nombre.get().strip() or not fecha.get().strip():
                messagebox.showerror("Error", "Todos los campos son obligatorios.")
                return
            partes = fecha.get().split("-")
            if len(partes) != 3 or not all(p.isdigit() for p in partes):
                messagebox.showerror("Error", "La fecha debe tener formato YYYY-MM-DD.")
                return
            ingresar_cliente(nombre.get(), sexo_var.get(), fecha.get())
            self.menu_screen()
        tk.Button(self, text="Guardar", command=ejecutar).pack(pady=5)
        tk.Button(self, text="Volver", command=self.menu_screen).pack()

    def pantalla_modificar_cliente(self):
        self.clear()
        tk.Label(self, text="Modificar Cliente", font=("Arial", 14)).pack(pady=10)
        form_frame = tk.Frame(self)
        form_frame.pack(pady=10)
        tk.Label(form_frame, text="ID Cliente:").grid(row=0, column=0, sticky="e", padx=5, pady=5)
        id_cliente = tk.Entry(form_frame)
        id_cliente.grid(row=0, column=1, padx=5, pady=5)
        tk.Label(form_frame, text="Nuevo Nombre:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
        nombre = tk.Entry(form_frame)
        nombre.grid(row=1, column=1, padx=5, pady=5)
        tk.Label(form_frame, text="Nuevo Sexo:").grid(row=2, column=0, sticky="e", padx=5, pady=5)
        sexo_var = tk.StringVar(value="Hombre")
        sexo_box = tk.OptionMenu(form_frame, sexo_var, "Hombre", "Mujer", "Otros")
        sexo_box.grid(row=2, column=1, padx=5, pady=5)
        tk.Label(form_frame, text="Nueva Fecha nacimiento (YYYY-MM-DD):").grid(row=3, column=0, sticky="e", padx=5, pady=5)
        fecha = tk.Entry(form_frame)
        fecha.grid(row=3, column=1, padx=5, pady=5)
        def ejecutar():
            if not id_cliente.get().strip() or not nombre.get().strip() or not fecha.get().strip():
                messagebox.showerror("Error", "Todos los campos son obligatorios.")
                return
            partes = fecha.get().split("-")
            if len(partes) != 3 or not all(p.isdigit() for p in partes):
                messagebox.showerror("Error", "La fecha debe tener formato YYYY-MM-DD.")
                return
            modificar_cliente(id_cliente.get(), nombre.get(), sexo_var.get(), fecha.get())
            self.menu_screen()
        tk.Button(self, text="Modificar", command=ejecutar).pack(pady=5)
        tk.Button(self, text="Volver", command=self.menu_screen).pack()

    def pantalla_eliminar_cliente(self):
        self.clear()
        tk.Label(self, text="Eliminar Cliente", font=("Arial", 14)).pack(pady=10)
        form_frame = tk.Frame(self)
        form_frame.pack(pady=10)
        tk.Label(form_frame, text="ID Cliente:").grid(row=0, column=0, sticky="e", padx=5, pady=5)
        id_cliente = tk.Entry(form_frame)
        id_cliente.grid(row=0, column=1, padx=5, pady=5)
        def ejecutar():
            if not id_cliente.get().strip():
                messagebox.showerror("Error", "El campo ID Cliente es obligatorio.")
                return
            eliminar_cliente(id_cliente.get())
            self.menu_screen()
        tk.Button(self, text="Eliminar", command=ejecutar).pack(pady=5)
        tk.Button(self, text="Volver", command=self.menu_screen).pack()

    def pantalla_ingresar_videojuego(self):
        self.clear()
        tk.Label(self, text="Ingresar Videojuego", font=("Arial", 14)).pack(pady=10)
        form_frame = tk.Frame(self)
        form_frame.pack(pady=10)
        tk.Label(form_frame, text="Nombre:").grid(row=0, column=0, sticky="e", padx=5, pady=5)
        nombre = tk.Entry(form_frame)
        nombre.grid(row=0, column=1, padx=5, pady=5)
        tk.Label(form_frame, text="Formato:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
        formato_var = tk.StringVar(value="Fisico")
        formato_box = tk.OptionMenu(form_frame, formato_var, "Fisico", "Digital")
        formato_box.grid(row=1, column=1, padx=5, pady=5)
        tk.Label(form_frame, text="Multiplayer:").grid(row=2, column=0, sticky="e", padx=5, pady=5)
        multiplayer_var = tk.StringVar(value="No")
        multiplayer_box = tk.OptionMenu(form_frame, multiplayer_var, "Si", "No")
        multiplayer_box.grid(row=2, column=1, padx=5, pady=5)
        tk.Label(form_frame, text="Valor:").grid(row=3, column=0, sticky="e", padx=5, pady=5)
        valor = tk.Entry(form_frame)
        valor.grid(row=3, column=1, padx=5, pady=5)
        tk.Label(form_frame, text="ID Empresa:").grid(row=4, column=0, sticky="e", padx=5, pady=5)
        empresa_id = tk.Entry(form_frame)
        empresa_id.grid(row=4, column=1, padx=5, pady=5)
        tk.Label(form_frame, text="ID Proveedor:").grid(row=5, column=0, sticky="e", padx=5, pady=5)
        proveedor_id = tk.Entry(form_frame)
        proveedor_id.grid(row=5, column=1, padx=5, pady=5)
        def ejecutar():
            multiplayer_val = '1' if multiplayer_var.get() == 'Si' else '0'
            ingresar_videojuego(nombre.get(), formato_var.get(), multiplayer_val, valor.get(), empresa_id.get(), proveedor_id.get())
            self.menu_screen()
        tk.Button(self, text="Guardar", command=ejecutar).pack(pady=5)
        tk.Button(self, text="Volver", command=self.menu_screen).pack()

    def pantalla_modificar_videojuego(self):
        self.clear()
        tk.Label(self, text="Modificar Videojuego", font=("Arial", 14)).pack(pady=10)
        form_frame = tk.Frame(self)
        form_frame.pack(pady=10)
        tk.Label(form_frame, text="ID Videojuego:").grid(row=0, column=0, sticky="e", padx=5, pady=5)
        id_videojuego = tk.Entry(form_frame)
        id_videojuego.grid(row=0, column=1, padx=5, pady=5)
        tk.Label(form_frame, text="Nuevo Nombre:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
        nombre = tk.Entry(form_frame)
        nombre.grid(row=1, column=1, padx=5, pady=5)
        tk.Label(form_frame, text="Nuevo Formato:").grid(row=2, column=0, sticky="e", padx=5, pady=5)
        formato_var = tk.StringVar(value="Fisico")
        formato_box = tk.OptionMenu(form_frame, formato_var, "Fisico", "Digital")
        formato_box.grid(row=2, column=1, padx=5, pady=5)
        tk.Label(form_frame, text="Multiplayer:").grid(row=3, column=0, sticky="e", padx=5, pady=5)
        multiplayer_var = tk.StringVar(value="No")
        multiplayer_box = tk.OptionMenu(form_frame, multiplayer_var, "Si", "No")
        multiplayer_box.grid(row=3, column=1, padx=5, pady=5)
        tk.Label(form_frame, text="Nuevo Valor:").grid(row=4, column=0, sticky="e", padx=5, pady=5)
        valor = tk.Entry(form_frame)
        valor.grid(row=4, column=1, padx=5, pady=5)
        tk.Label(form_frame, text="Nuevo ID Empresa:").grid(row=5, column=0, sticky="e", padx=5, pady=5)
        empresa_id = tk.Entry(form_frame)
        empresa_id.grid(row=5, column=1, padx=5, pady=5)
        tk.Label(form_frame, text="Nuevo ID Proveedor:").grid(row=6, column=0, sticky="e", padx=5, pady=5)
        proveedor_id = tk.Entry(form_frame)
        proveedor_id.grid(row=6, column=1, padx=5, pady=5)
        def ejecutar():
            multiplayer_val = '1' if multiplayer_var.get() == 'Si' else '0'
            modificar_videojuego(id_videojuego.get(), nombre.get(), formato_var.get(), multiplayer_val, valor.get(), empresa_id.get(), proveedor_id.get())
            self.menu_screen()
        tk.Button(self, text="Guardar", command=ejecutar).pack(pady=5)
        tk.Button(self, text="Volver", command=self.menu_screen).pack()

    def pantalla_listar_videojuegos(self):
        self.clear()
        tk.Label(self, text="Lista de Videojuegos", font=("Arial", 14)).pack(pady=10)
        text_area = scrolledtext.ScrolledText(self, width=50, height=20)
        text_area.pack(fill="both", expand=True)
        videojuegos = listar_videojuegos()
        for row in videojuegos:
            text_area.insert(tk.END, f"ID: {row[0]} | Nombre: {row[1]} | Formato: {row[2]} | Multiplayer: {row[3]} | Valor: {row[4]} | Empresa ID: {row[5]} | Proveedor ID: {row[6]}\n")
        tk.Button(self, text="Volver", command=self.menu_screen).pack(pady=5)

    def pantalla_eliminar_videojuego(self):
        self.clear()
        tk.Label(self, text="Eliminar Videojuego", font=("Arial", 14)).pack(pady=10)
        form_frame = tk.Frame(self)
        form_frame.pack(pady=10)
        tk.Label(form_frame, text="ID Videojuego:").grid(row=0, column=0, sticky="e", padx=5, pady=5)
        id_videojuego = tk.Entry(form_frame)
        id_videojuego.grid(row=0, column=1, padx=5, pady=5)
        def ejecutar():
            eliminar_videojuego(id_videojuego.get())
            self.menu_screen()
        tk.Button(self, text="Eliminar", command=ejecutar).pack(pady=5)
        tk.Button(self, text="Volver", command=self.menu_screen).pack()

    def pantalla_listar_clientes(self):
        self.clear()
        tk.Label(self, text="Lista de Clientes", font=("Arial", 14)).pack(pady=10)
        text_area = scrolledtext.ScrolledText(self, width=50, height=10)
        text_area.pack(fill="both", expand=True)
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_cliente, nombre, sexo, fecha_nacimiento FROM clientes ORDER BY id_cliente")
                clientes = cur.fetchall()
        for row in clientes:
            text_area.insert(tk.END, f"ID: {row[0]} | Nombre: {row[1]} | Sexo: {row[2]} | Fecha Nacimiento: {row[3]}\n")
        tk.Button(self, text="Volver", command=self.menu_screen).pack(pady=5)

    def pantalla_listar_empresas(self):
        self.clear()
        tk.Label(self, text="Lista de Empresas", font=("Arial", 14)).pack(pady=10)
        text_area = scrolledtext.ScrolledText(self, width=50, height=10)
        text_area.pack(fill="both", expand=True)
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_empresa, nombre FROM empresas ORDER BY id_empresa")
                empresas = cur.fetchall()
        for row in empresas:
            text_area.insert(tk.END, f"ID: {row[0]} | Nombre: {row[1]}\n")
        tk.Button(self, text="Volver", command=self.menu_screen).pack(pady=5)

    def pantalla_listar_proveedores(self):
        self.clear()
        tk.Label(self, text="Lista de Proveedores", font=("Arial", 14)).pack(pady=10)
        text_area = scrolledtext.ScrolledText(self, width=50, height=10)
        text_area.pack(fill="both", expand=True)
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_proveedor, precios FROM proveedor ORDER BY id_proveedor")
                proveedores = cur.fetchall()
        for row in proveedores:
            text_area.insert(tk.END, f"ID: {row[0]} | Precio: {row[1]}\n")
        tk.Button(self, text="Volver", command=self.menu_screen).pack(pady=5)

    def ejecutar_insercion_masiva(self):
        ejecutar_insercion()
        self.menu_screen()

if __name__ == "__main__":
    app = App()
    app.mainloop()