import tkinter as tk
from tkinter import messagebox
import os
import platform

def cambiar_hora():
    hora = entrada_hora.get()
    minuto = entrada_minuto.get()

    if not hora.isdigit() or not minuto.isdigit():
        messagebox.showerror("Error", "Ingresa valores numéricos válidos.")
        return

    hora = int(hora)
    minuto = int(minuto)

    if not (0 <= hora < 24 and 0 <= minuto < 60):
        messagebox.showerror("Error", "Hora o minuto fuera de rango.")
        return

    hora_formato = f"{hora:02d}:{minuto:02d}:00"

    sistema = platform.system()

    if sistema == "Windows":
        os.system(f"time {hora_formato}")
    elif sistema in ["Linux", "Darwin"]:  # Darwin = macOS
        os.system(f"sudo date +%T -s \"{hora_formato}\"")
    else:
        messagebox.showerror("Error", "Sistema operativo no compatible.")

# Interfaz
ventana = tk.Tk()
ventana.title("Cambiar Hora del Sistema")

tk.Label(ventana, text="Hora (0-23):").grid(row=0, column=0, padx=10, pady=10)
entrada_hora = tk.Entry(ventana, width=5)
entrada_hora.grid(row=0, column=1)

tk.Label(ventana, text="Minuto (0-59):").grid(row=1, column=0, padx=10, pady=10)
entrada_minuto = tk.Entry(ventana, width=5)
entrada_minuto.grid(row=1, column=1)

boton_cambiar = tk.Button(ventana, text="Cambiar Hora", command=cambiar_hora, bg="blue", fg="white", font=("Arial", 12))
boton_cambiar.grid(row=2, column=0, columnspan=2, pady=20)

ventana.mainloop()
