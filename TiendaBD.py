import os
import psycopg2
import matplotlib.pyplot as plt
import numpy as np

def graficar_ventas(n, ano):
    try:
        conn = psycopg2.connect(
            dbname="base_datos",
            user="postgres",
            password="0987",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()
        
        if n == 1:
            # Obtener todas las regiones
            cursor.execute("SELECT region_nombre FROM region ORDER BY region_nombre;")
            todas_regiones = [r[0] for r in cursor.fetchall()]

            # Obtener ingresos por formato y región (incluyendo regiones sin ventas)
            query = """
                SELECT
                    r.region_nombre AS region,
                    f.formato AS formato_juego,
                    COALESCE(SUM(hv.precio_clp), 0) AS ingresos_totales_clp
                FROM region r
                CROSS JOIN formato f
                LEFT JOIN hechos_ventas hv ON hv.id_region = r.id_region AND hv.id_formato = f.id_formato AND EXTRACT(YEAR FROM hv.fecha_compra) = %s
                GROUP BY r.region_nombre, f.formato
                ORDER BY r.region_nombre, f.formato;
            """
            cursor.execute(query, (ano,))
            resultados = cursor.fetchall()

            if not resultados:
                print("⚠️ No hay datos disponibles para este año.")
                return

            # Organizar resultados en: {region: {"Fisico": valor, "Digital": valor}}
            datos = {region: {"Fisico": 0, "Digital": 0} for region in todas_regiones}
            for region, formato, ingresos in resultados:
                datos[region][formato.capitalize()] = ingresos

            regiones = sorted(datos.keys())
            ingresos_fisicos = [datos[r].get("Fisico", 0) for r in regiones]
            ingresos_digitales = [datos[r].get("Digital", 0) for r in regiones]

            # Crear gráfico
            posiciones = np.arange(len(regiones))
            ancho = 0.35

            plt.figure(figsize=(14, 6))
            plt.bar(posiciones - ancho/2, ingresos_fisicos, width=ancho, label='Físico', color='blue')
            plt.bar(posiciones + ancho/2, ingresos_digitales, width=ancho, label='Digital', color='green')

            plt.xticks(posiciones, regiones, rotation=45, ha='right')
            plt.xlabel('Región')
            plt.ylabel('Ingresos CLP')
            plt.title(f'Ingresos por Formato y Región - Año {ano}')
            plt.legend()
            plt.tight_layout()

            # Guardar gráfico
            carpeta = os.path.join("graficos", str(ano))
            os.makedirs(carpeta, exist_ok=True)
            archivo = os.path.join(carpeta, f'ingresos_por_formato_region_{ano}.png')
            plt.savefig(archivo)
            plt.close()
            print(f"Gráfico guardado en: {archivo}")

        elif n == 2:
            # Mostrar los 2 géneros más comprados por cada región
            query = """
                SELECT region, nombre_genero, total FROM (
                    SELECT 
                        r.region_nombre AS region,
                        g.nombre_genero,
                        COUNT(*) AS total,
                        ROW_NUMBER() OVER (PARTITION BY r.region_nombre ORDER BY COUNT(*) DESC) as rn
                    FROM hechos_ventas hv
                    JOIN region r ON hv.id_region = r.id_region
                    JOIN genero g ON hv.id_genero = g.id_genero
                    WHERE EXTRACT(YEAR FROM hv.fecha_compra) = %s
                    GROUP BY r.region_nombre, g.nombre_genero
                ) t
                WHERE rn <= 2
                ORDER BY region, total DESC;
            """
            cursor.execute(query, (ano,))
            resultados = cursor.fetchall()

            if not resultados:
                print("No hay datos disponibles para este análisis.")
                return

            # Organizar resultados: {region: [(genero, total), (genero, total)]}
            datos = {}
            for region, genero, total in resultados:
                if region not in datos:
                    datos[region] = []
                datos[region].append((genero, total))

            regiones = list(datos.keys())
            generos_1 = [datos[r][0][0] if len(datos[r]) > 0 else '' for r in regiones]
            generos_2 = [datos[r][1][0] if len(datos[r]) > 1 else '' for r in regiones]
            totales_1 = [datos[r][0][1] if len(datos[r]) > 0 else 0 for r in regiones]
            totales_2 = [datos[r][1][1] if len(datos[r]) > 1 else 0 for r in regiones]

            posiciones = np.arange(len(regiones))
            ancho = 0.35

            plt.figure(figsize=(16, 7))
            barras1 = plt.bar(posiciones - ancho/2, totales_1, width=ancho, label='1er Género', color='blue')
            barras2 = plt.bar(posiciones + ancho/2, totales_2, width=ancho, label='2do Género', color='green')

            plt.xticks(posiciones, regiones, rotation=45, ha='right')
            plt.ylabel('Total de Compras')
            plt.xlabel('Región')
            plt.title(f'Los 2 géneros más comprados por región - Año {ano}')
            plt.legend()
            plt.tight_layout()

            # Etiquetas con el nombre del género sobre cada barra (vertical y en negrita)
            for barra, genero in zip(barras1, generos_1):
                plt.text(barra.get_x() + barra.get_width()/2, barra.get_height() + 0.5,
                        genero, ha='center', va='bottom', fontsize=9, color='black', rotation=90, fontweight='bold')
            for barra, genero in zip(barras2, generos_2):
                plt.text(barra.get_x() + barra.get_width()/2, barra.get_height() + 0.5,
                        genero, ha='center', va='bottom', fontsize=9, color='black', rotation=90, fontweight='bold')

            carpeta = os.path.join("graficos", str(ano))
            os.makedirs(carpeta, exist_ok=True)
            archivo = os.path.join(carpeta, f'top2_generos_por_region_{ano}.png')
            plt.savefig(archivo)
            plt.close()
            print(f"Gráfico guardado en: {archivo}")

        elif n == 3:
            query = """
                SELECT
                    re.rango,
                    COUNT(*) AS total_compras
                FROM hechos_ventas hv
                JOIN rango_etario re ON hv.id_rango_etario = re.id_rango_etario
                WHERE hv.id_multiplayer = 1
                AND EXTRACT(YEAR FROM hv.fecha_compra) = %s
                GROUP BY re.rango
                ORDER BY total_compras DESC;
            """
            cursor.execute(query, (ano,))
            resultados = cursor.fetchall()

            if not resultados:
                return  # No hay datos, no grafica

            rangos = [r for r, _ in resultados]
            totales = [t for _, t in resultados]

            posiciones = np.arange(len(rangos))
            plt.figure(figsize=(10, 6))
            barras = plt.barh(posiciones, totales, color='#E67300')

            plt.yticks(posiciones, rangos)
            plt.xlabel('Total de Compras')
            plt.title(f"Total de Compras de Juegos Multijugador por Rango Etario - Año {ano}")

            for barra, total in zip(barras, totales):
                plt.text(barra.get_width() + 1, barra.get_y() + barra.get_height()/2,
                        str(total), va='center', ha='left', fontsize=9, color='white')

            plt.tight_layout()

            # Crear carpeta y guardar gráfico
            carpeta = os.path.join("graficos", str(ano))
            os.makedirs(carpeta, exist_ok=True)
            archivo = os.path.join(carpeta, f'compras_multijugador_por_rango_{ano}.png')
            plt.savefig(archivo)
            plt.close()
        
        elif n == 4:
            # Gráfico de línea: Total de ventas por mes, con dos líneas (HOMBRES y MUJERES)
            # Ajuste: Usar id_sexo directamente desde hechos_ventas (si existe)
            query = """
                SELECT 
                    EXTRACT(MONTH FROM hv.fecha_compra) AS mes,
                    CASE hv.id_sexo
                        WHEN 1 THEN 'Hombres'
                        WHEN 2 THEN 'Mujeres'
                        ELSE 'Otro'
                    END AS genero_cliente,
                    COUNT(*) AS total_ventas
                FROM hechos_ventas hv
                WHERE EXTRACT(YEAR FROM hv.fecha_compra) = %s
                GROUP BY mes, genero_cliente
                ORDER BY mes, genero_cliente;
            """
            cursor.execute(query, (ano,))
            resultados = cursor.fetchall()

            # Inicializar estructura para 12 meses
            meses = list(range(1, 13))
            ventas_hombres = [0] * 12
            ventas_mujeres = [0] * 12

            for mes, genero, total in resultados:
                idx = int(mes) - 1
                if genero == 'Hombres':
                    ventas_hombres[idx] = total
                elif genero == 'Mujeres':
                    ventas_mujeres[idx] = total

            nombres_meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]

            plt.figure(figsize=(14, 7))
            plt.plot(nombres_meses, ventas_hombres, marker='o', color='blue', linewidth=2, label='Hombres')
            plt.plot(nombres_meses, ventas_mujeres, marker='o', color='magenta', linewidth=2, label='Mujeres')
            for x, y in zip(nombres_meses, ventas_hombres):
                plt.text(x, y, str(y), ha='center', va='bottom', fontsize=9, color='blue', fontweight='bold')
            for x, y in zip(nombres_meses, ventas_mujeres):
                plt.text(x, y, str(y), ha='center', va='bottom', fontsize=9, color='magenta', fontweight='bold')
            plt.ylabel('Total de Ventas')
            plt.xlabel('Mes')
            plt.title(f"Total de ventas por mes y género (Hombres/Mujeres) - Año {ano}")
            plt.legend()
            plt.tight_layout()
            plt.subplots_adjust(bottom=0.2)

            carpeta = os.path.join("graficos", str(ano))
            os.makedirs(carpeta, exist_ok=True)
            archivo = os.path.join(carpeta, f'ventas_por_mes_hombres_mujeres_{ano}.png')
            plt.savefig(archivo)
            plt.close()
            print(f"Gráfico guardado en: {archivo}")

        elif n == 5:
            query = """
                SELECT 
                    EXTRACT(MONTH FROM hv.fecha_compra) AS mes,
                    f.formato AS formato_juego,
                    SUM(hv.precio_clp) AS ingresos_totales_clp
                FROM hechos_ventas hv
                JOIN formato f ON hv.id_formato = f.id_formato
                WHERE EXTRACT(YEAR FROM hv.fecha_compra) = %s
                GROUP BY f.formato, mes
                ORDER BY mes, ingresos_totales_clp DESC;
            """
            cursor.execute(query, (ano,))
            resultados = cursor.fetchall()
            if not resultados:
                print("No hay datos para graficar.")
                return
            
            meses = list(range(1, 13))
            ingresos_fisicos = [0] * 12
            ingresos_digitales = [0] * 12

            for mes, formato, ingresos in resultados:
                idx = int(mes) - 1
                if formato == 'Fisico':
                    ingresos_fisicos[idx] = ingresos
                elif formato == 'Digital':
                    ingresos_digitales[idx] = ingresos
            posiciones = np.arange(len(meses))
            ancho = 0.35

            plt.figure(figsize=(12, 6))
            plt.bar(posiciones - ancho/2, ingresos_fisicos, width=ancho, label='Físico', color='blue')
            plt.bar(posiciones + ancho/2, ingresos_digitales, width=ancho, label='Digital', color='green')
            plt.xticks(posiciones,["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"])
            plt.xlabel('Mes')
            plt.ylabel('Ingresos CLP')
            plt.title(f'Ingresos por formato en cada mes - Año {ano}')
            plt.legend()
            plt.tight_layout()

            # Crear carpeta si no existe
            carpeta = os.path.join("graficos", str(ano))
            os.makedirs(carpeta, exist_ok=True)
            archivo = os.path.join(carpeta, f'ingresos_por_formato_mensual_{ano}.png')
            plt.savefig(archivo)
            plt.close()
            print(f"Gráfico guardado en: {archivo}")
        
        elif n == 6:
            query = """
                SELECT 
                    g.nombre_genero AS genero,
                    f.formato AS formato,
                    COUNT(*) AS total
                FROM hechos_ventas hv
                JOIN genero g ON hv.id_genero = g.id_genero
                JOIN formato f ON hv.id_formato = f.id_formato
                WHERE EXTRACT(YEAR FROM hv.fecha_compra) = %s
                GROUP BY g.nombre_genero, f.formato
                ORDER BY g.nombre_genero, total DESC;
            """
            cursor.execute(query, (ano,))
            resultados = cursor.fetchall()

            if not resultados:
                print("No hay datos disponibles para este análisis.")
                return

            # Procesar resultados en un diccionario: {genero: {Fisico: x, Digital: y}}
            datos = {}
            for genero, formato, total in resultados:
                if genero not in datos:
                    datos[genero] = {"Fisico": 0, "Digital": 0}
                datos[genero][formato.capitalize()] = total

            generos = sorted(datos.keys())
            fisico = [datos[g].get("Fisico", 0) for g in generos]
            digital = [datos[g].get("Digital", 0) for g in generos]

            posiciones = np.arange(len(generos))
            ancho = 0.35

            plt.figure(figsize=(14, 6))
            plt.bar(posiciones - ancho/2, fisico, width=ancho, label='Físico', color='blue')
            plt.bar(posiciones + ancho/2, digital, width=ancho, label='Digital', color='green')
            plt.xticks(posiciones, generos, rotation=45, ha='right')
            plt.xlabel('Género de Videojuego')
            plt.ylabel('Total de Ventas')
            plt.title(f'Formato más popular según género - Año {ano}')
            plt.legend()
            plt.tight_layout()

            carpeta = os.path.join("graficos", str(ano))
            os.makedirs(carpeta, exist_ok=True)
            archivo = os.path.join(carpeta, f'formato_por_genero_{ano}.png')
            plt.savefig(archivo)
            plt.close()
            print(f"Gráfico guardado en: {archivo}")

        else:
            print("Esa opción aún no está implementada.")

        cursor.close()
        conn.close()

    except Exception as e:
        print("Error al conectar o consultar la base de datos:", e)

if __name__ == "__main__":
    while True:
        try:
            ano = int(input("Ingrese el año para filtrar las ventas (por ejemplo, 2023): "))
            if 2020 <= ano <= 2025:
                break
            else:
                print("Ingrese un año válido entre 2020 y 2025.")
        except ValueError:
            print("Por favor, ingrese un número válido para el año.")

    # Generar los 6 gráficos automáticamente
    for n in range(1, 7):
        print(f"\nGenerando gráfico {n}...")
        graficar_ventas(n, ano)
    print("\nTodos los gráficos han sido generados.")
