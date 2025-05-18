import os
import psycopg2
import matplotlib.pyplot as plt
import numpy as np

def graficar_ventas(n, ano):
    try:
        conn = psycopg2.connect(
            dbname="venta_juegos",
            user="admin",
            password="Admin132",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()
        
        if n == 1:
            query = """
                SELECT
                    s.region AS region,
                    j.formato AS formato_juego,
                    SUM(hv.precio_clp) AS ingresos_totales_clp
                FROM hechos_ventas hv
                JOIN juego j ON hv.id_juego = j.id_juego
                JOIN sucursal s ON hv.id_sucursal = s.id_sucursal
                WHERE EXTRACT(YEAR FROM hv.fecha_compra) = %s
                GROUP BY s.region, j.formato
                ORDER BY s.region, ingresos_totales_clp DESC;
            """
            cursor.execute(query, (ano,))
            resultados = cursor.fetchall()

            regiones = {}
            for region, formato, ingresos in resultados:
                if region not in regiones:
                    regiones[region] = {"Fisico":0, "Digital":0}
                regiones[region][formato.capitalize()] = ingresos

            regiones_ordenadas = sorted(regiones.keys())
            ingresos_fisicos = [regiones[r].get("Fisico", 0) for r in regiones_ordenadas]
            ingresos_digitales = [regiones[r].get("Digital", 0) for r in regiones_ordenadas]

            posiciones = np.arange(len(regiones_ordenadas))
            ancho = 0.35

            plt.figure(figsize=(12,6))
            plt.bar(posiciones - ancho/2, ingresos_fisicos, width=ancho, label='Físico', color='blue')
            plt.bar(posiciones + ancho/2, ingresos_digitales, width=ancho, label='Digital', color='green')
            plt.xticks(posiciones, regiones_ordenadas, rotation=45, ha='right')
            plt.xlabel('Región')
            plt.ylabel('Ingresos CLP')
            plt.title(f'Ingresos por formato en todas las regiones - Año {ano}')
            plt.legend()
            plt.tight_layout()

            # Crear carpeta si no existe
            carpeta = "graficos"
            if not os.path.exists(carpeta):
                os.makedirs(carpeta)

            # Guardar gráfico en carpeta con nombre
            archivo = os.path.join(carpeta, f'ingresos_por_formato_{ano}.png')
            plt.savefig(archivo)
            plt.close()

            print(f"Gráfico guardado en: {archivo}")
        elif n == 2:
            query = """
                SELECT
                    s.region,
                    j.genero,
                    COUNT(*) AS total_ventas,
                    ROW_NUMBER() OVER (PARTITION BY s.region ORDER BY COUNT(*) DESC) AS rank
                FROM hechos_ventas hv
                JOIN juego j ON hv.id_juego = j.id_juego
                JOIN sucursal s ON hv.id_sucursal = s.id_sucursal
                WHERE EXTRACT(YEAR FROM hv.fecha_compra) = %s
                GROUP BY s.region, j.genero
                HAVING COUNT(*) > 0
                ORDER BY s.region, total_ventas DESC;
            """
            cursor.execute(query, (ano,))
            resultados = cursor.fetchall()

            if not resultados:
                return  # No se hace nada si no hay resultados

            # Diccionario para almacenar por región el top 1 y top 2
            datos_region = {}
            for region, genero, total, rank in resultados:
                if region not in datos_region:
                    datos_region[region] = {1: None, 2: None}
                if rank in (1, 2):
                    datos_region[region][rank] = (genero, total)

            regiones = sorted(datos_region.keys())

            top1_ventas = []
            top1_generos = []
            top2_ventas = []
            top2_generos = []

            for region in regiones:
                top1 = datos_region[region][1]
                top2 = datos_region[region][2]

                if top1:
                    top1_generos.append(top1[0])
                    top1_ventas.append(top1[1])
                else:
                    top1_generos.append("")
                    top1_ventas.append(0)

                if top2:
                    top2_generos.append(top2[0])
                    top2_ventas.append(top2[1])
                else:
                    top2_generos.append("")
                    top2_ventas.append(0)

            posiciones = np.arange(len(regiones))
            ancho = 0.4

            plt.figure(figsize=(14, 7))

            barras1 = plt.barh(posiciones - ancho/2, top1_ventas, height=ancho, color='black', label='Género más vendido')
            barras2 = plt.barh(posiciones + ancho/2, top2_ventas, height=ancho, color='purple', label='Segundo género más vendido')

            plt.yticks(posiciones, regiones)
            plt.xlabel('Cantidad de Ventas')
            plt.title(f"Géneros más vendidos por región - Año {ano}")
            plt.legend()

            # Texto dentro de barras - top1 (negro, texto blanco)
            for barra, genero in zip(barras1, top1_generos):
                if barra.get_width() > 0:
                    plt.text(barra.get_width() * 0.02, barra.get_y() + barra.get_height()/2,
                            genero, va='center', ha='left', fontsize=9, color='white')

            # Texto dentro de barras - top2 (morado, texto blanco)
            for barra, genero in zip(barras2, top2_generos):
                if barra.get_width() > 0:
                    plt.text(barra.get_width() * 0.02, barra.get_y() + barra.get_height()/2,
                            genero, va='center', ha='left', fontsize=9, color='white')

            plt.tight_layout()

            carpeta = "graficos"
            os.makedirs(carpeta, exist_ok=True)

            archivo = os.path.join(carpeta, f'generos_top2_por_region_{ano}.png')
            plt.savefig(archivo)
            plt.close()
        elif n == 3:
            query = """
                SELECT
                    re.rango,
                    COUNT(*) AS total_compras
                FROM hechos_ventas hv
                JOIN juego j ON hv.id_juego = j.id_juego
                JOIN rango_etario re ON hv.id_rango_etario = re.id_rango_etario
                WHERE j.multiplayer = 'Si'
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
            carpeta = "graficos"
            os.makedirs(carpeta, exist_ok=True)
            archivo = os.path.join(carpeta, f'compras_multijugador_por_rango_{ano}.png')
            plt.savefig(archivo)
            plt.close()
        
        elif n == 4:
            query = """
                SELECT count(id_juego) AS juegos_vendidos, e.nombre AS nombre_estudio FROM hechos_ventas hv
                JOIN estudio e ON hv.id_estudio = e.id_estudio
                WHERE EXTRACT(YEAR FROM hv.fecha_compra) = %s
                GROUP BY e.id_estudio
            """
            cursor.execute(query, (ano,))
            resultados = cursor.fetchall()

            if not resultados:
                return  # No hay datos, no grafica

            estudios = [nombre for _, nombre in resultados]
            juegos_vendidos = [cantidad for cantidad, _ in resultados]

            posiciones = np.arange(len(estudios))
            plt.figure(figsize=(12, 6))
            barras = plt.bar(posiciones, juegos_vendidos, color='#E67300')

            plt.xticks(posiciones, estudios, rotation=45, ha='right')
            plt.ylabel('Cantidad de Juegos Vendidos')
            plt.xlabel('Estudio')
            plt.title(f"Total de Juegos Vendidos por Estudio - Año {ano}")

            for barra, total in zip(barras, juegos_vendidos):
                plt.text(barra.get_x() + barra.get_width()/2, barra.get_height() + 1,
                        str(total), va='bottom', ha='center', fontsize=9, color='black')

            plt.tight_layout()

            # Crear carpeta y guardar gráfico
            carpeta = "graficos"
            os.makedirs(carpeta, exist_ok=True)
            archivo = os.path.join(carpeta, f'juegos_vendidos_por_estudio_{ano}.png')
            plt.savefig(archivo)
            plt.close()
        elif n == 5:
            query = """
            select 
                EXTRACT(MONTH FROM fecha_compra) AS mes,
                j.formato AS formato_juego,
                SUM(hv.precio_clp) AS ingresos_totales_clp
            from hechos_ventas hv
            JOIN juego j ON hv.id_juego = j.id_juego
            JOIN sucursal s ON hv.id_sucursal = s.id_sucursal
            WHERE EXTRACT(YEAR FROM hv.fecha_compra) = %s
            GROUP BY j.formato, mes
            order by mes, ingresos_totales_clp desc
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
            carpeta = "graficos"
            os.makedirs(carpeta, exist_ok=True)
            archivo = os.path.join(carpeta, f'ingresos_por_formato_mensual_{ano}.png')
            plt.savefig(archivo)
            plt.close()
            print(f"Gráfico guardado en: {archivo}")
        
        elif n == 6:
            query = """
                SELECT 
                    j.genero,
                    j.formato,
                    COUNT(*) AS total
                FROM hechos_ventas hv
                JOIN juego j ON hv.id_juego = j.id_juego
                WHERE EXTRACT(YEAR FROM hv.fecha_compra) = %s
                GROUP BY j.genero, j.formato
                ORDER BY j.genero, total DESC;
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

            carpeta = "graficos"
            os.makedirs(carpeta, exist_ok=True)
            archivo = os.path.join(carpeta, f'formato_por_genero{ano}.png')
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
    n = 0
    while n < 1 or n > 6:
        try:
            n = int(input("Ingrese el número de la opción deseada:\n"
                "1. Ingresos por formato y región\n"
                "2. Ingresos históricos por formato de juego en cada región\n"
                "3. Total de compras multijugador por rango etario\n"
                "4. Total de juegos vendidos por estudio de videojuegos\n"
                "5. Total de juegos vendidos por mes en formato fisico y digital\n"
                "6. Formato mas vendido por genero de videojuegos\n"
            ))
        except ValueError:
            print("Por favor, ingrese un número válido.")
            continue
    while True:
        try:
            ano = int(input("Ingrese el año para filtrar las ventas (por ejemplo, 2023): "))
            if 2020 <= ano <= 2025:
                break
            else:
                print("Ingrese un año válido entre 2020 y 2025.")
        except ValueError:
            print("Por favor, ingrese un número válido para el año.")

    graficar_ventas(n, ano)
