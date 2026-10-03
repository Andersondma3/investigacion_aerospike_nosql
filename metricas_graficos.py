import aerospike
from aerospike import exception as ex
import os
import time
import numpy as np
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib.patches import Patch
from concurrent.futures import ThreadPoolExecutor, as_completed


# Configuración de Aerospike 

_EN_DOCKER = os.path.exists("/.dockerenv")

HOST = os.getenv(
    "AEROSPIKE_HOST",
    "aerospike" if _EN_DOCKER else "127.0.0.1"
)

PORT = int(os.getenv("AEROSPIKE_PORT", "3000"))
NAMESPACE = "test"

config = {
    "hosts": [(HOST, PORT)]
}

# Datos seleccionados para las prueba

HILOS_PRUEBA = [1, 8, 16, 32, 64]
OPERACIONES_POR_PRUEBA = 5000

EVENTO = 1
ZONA = "VIP"

CARPETA_RESULTADOS = "resultados"

CARPETA_GRAFICOS = os.path.join(
    CARPETA_RESULTADOS,
    "graficos"
)

# Configuración para los gráficos

COLORES = {
    "Lectura": "#79C7FF",
    "Escritura": "#1E88E5",
    "Actualización atómica": "#0E4D92"
}

TAMANO_FIGURA = (9, 5.5)

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 10,
    "axes.titlesize": 14,
    "axes.labelsize": 11,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 10,
    "figure.facecolor": "white",
    "axes.facecolor": "white"
})


os.makedirs(
    CARPETA_RESULTADOS,
    exist_ok=True
)

os.makedirs(
    CARPETA_GRAFICOS,
    exist_ok=True
)

def limpiar_resultados():

    for archivo in os.listdir(
        CARPETA_GRAFICOS
    ):

        if archivo.lower().endswith(".png"):

            ruta = os.path.join(
                CARPETA_GRAFICOS,
                archivo
            )

            os.remove(
                ruta
            )

    csv_anterior = os.path.join(
        CARPETA_RESULTADOS,
        "metricas_rendimiento.csv"
    )

    if os.path.exists(
        csv_anterior
    ):

        os.remove(
            csv_anterior
        )

def aplicar_estilo(
    ax,
    eje_grid="y"
):

    ax.grid(
        axis=eje_grid,
        linestyle="--",
        linewidth=0.7,
        alpha=0.18
    )

    ax.set_axisbelow(
        True
    )

    ax.spines["top"].set_visible(
        False
    )

    ax.spines["right"].set_visible(
        False
    )

    ax.spines["left"].set_alpha(
        0.4
    )

    ax.spines["bottom"].set_alpha(
        0.4
    )

def agregar_leyenda(
    ax,
    operaciones
):

    elementos = [
        Patch(
            facecolor=COLORES[operacion],
            edgecolor="none",
            label=operacion
        )
        for operacion in operaciones
    ]

    ax.legend(
        handles=elementos,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.15),
        ncol=len(operaciones),
        frameon=False,
        handlelength=1.2,
        columnspacing=2
    )

def conectar():

    return aerospike.client(
        config
    ).connect()


# Calculamos las métricas

def calcular_metricas(
    nombre,
    hilos,
    latencias,
    exitos,
    fallos,
    tiempo_total
):

    if not latencias:

        return {
            "operacion": nombre,
            "hilos": hilos,
            "exitos": exitos,
            "fallos": fallos,
            "tiempo_total_s": tiempo_total,
            "ops_s": 0,
            "latencia_promedio_ms": 0,
            "p50_ms": 0,
            "p95_ms": 0,
            "p99_ms": 0
        }


    return {
        "operacion": nombre,
        "hilos": hilos,
        "exitos": exitos,
        "fallos": fallos,
        "tiempo_total_s": tiempo_total,
        "ops_s": exitos / tiempo_total,
        "latencia_promedio_ms": np.mean(latencias),
        "p50_ms": np.percentile(latencias, 50),
        "p95_ms": np.percentile(latencias, 95),
        "p99_ms": np.percentile(latencias, 99)
    }

def ejecutar_prueba(
    funcion,
    hilos
):

    latencias = []

    exitos = 0
    fallos = 0

    inicio_total = time.perf_counter()


    with ThreadPoolExecutor(
        max_workers=hilos
    ) as executor:

        futuros = [
            executor.submit(
                funcion,
                numero
            )
            for numero in range(
                OPERACIONES_POR_PRUEBA
            )
        ]


        for futuro in as_completed(
            futuros
        ):

            latencia, exito = futuro.result()


            if exito:

                exitos += 1
                latencias.append(
                    latencia
                )

            else:

                fallos += 1


    tiempo_total = (
        time.perf_counter()
        - inicio_total
    )

    return (
        latencias,
        exitos,
        fallos,
        tiempo_total
    )

def crear_operacion_lectura(
    client
):

    key = (
        NAMESPACE,
        "inventario",
        f"evt:{EVENTO}:zona:{ZONA}"
    )

    def leer(numero):

        inicio = time.perf_counter()

        try:

            client.get(
                key
            )

            exito = True

        except ex.AerospikeError:

            exito = False

        latencia = (
            time.perf_counter()
            - inicio
        ) * 1000


        return latencia, exito

    return leer

def crear_operacion_escritura(
    client,
    hilos
):

    def escribir(numero):

        key = (
            NAMESPACE,
            "metricas_escritura",
            f"h{hilos}:registro:{numero}"
        )


        datos = {
            "numero": numero,
            "hilos": hilos,
            "valor": numero * 2
        }

        inicio = time.perf_counter()

        try:

            client.put(
                key,
                datos
            )

            exito = True

        except ex.AerospikeError:

            exito = False


        latencia = (
            time.perf_counter()
            - inicio
        ) * 1000


        return latencia, exito


    return escribir

def preparar_contador_cas(
    client,
    hilos
):

    key = (
        NAMESPACE,
        "metricas_cas",
        f"contador:hilos:{hilos}"
    )


    client.put(
        key,
        {
            "contador": 0
        }
    )

    return key

def crear_operacion_cas(
    client,
    key
):
    
    def actualizar(numero):

        inicio = time.perf_counter()

        reintentos = 0
        exito = False


        while reintentos < 100:

            try:

                _, meta, datos = client.get(
                    key
                )

                contador_actual = datos.get(
                    "contador",
                    0
                )

                datos["contador"] = (
                    contador_actual + 1
                )

                try:

                    client.put(
                        key,
                        datos,
                        policy={
                            "gen": aerospike.POLICY_GEN_EQ
                        },
                        meta={
                            "gen": meta["gen"]
                        }
                    )

                    exito = True
                    break

                except ex.RecordGenerationError:

                    reintentos += 1

                    time.sleep(
                        0.0005
                    )

            except ex.AerospikeError:

                break

        latencia = (
            time.perf_counter()
            - inicio
        ) * 1000

        return latencia, exito

    return actualizar

def mostrar_resultado(
    resultado
):

    print(
        f"\n{resultado['operacion']} "
        f"- {resultado['hilos']} hilos"
    )

    print(
        f"Operaciones correctas: "
        f"{resultado['exitos']:,}"
    )

    print(
        f"Operaciones fallidas:  "
        f"{resultado['fallos']:,}"
    )

    print(
        f"Tiempo total:          "
        f"{resultado['tiempo_total_s']:.3f} s"
    )

    print(
        f"Operaciones/segundo:   "
        f"{resultado['ops_s']:.2f}"
    )

    print(
        f"Latencia promedio:     "
        f"{resultado['latencia_promedio_ms']:.3f} ms"
    )

    print(
        f"p50:                   "
        f"{resultado['p50_ms']:.3f} ms"
    )

    print(
        f"p95:                   "
        f"{resultado['p95_ms']:.3f} ms"
    )

    print(
        f"p99:                   "
        f"{resultado['p99_ms']:.3f} ms"
    )
# Gráfico 1
# Rendimiento según concurrencia

def grafico_rendimiento(
    resultados
):

    fig, ax = plt.subplots(
        figsize=TAMANO_FIGURA
    )


    operaciones = [
        "Lectura",
        "Escritura",
        "Actualización atómica"
    ]


    for operacion in operaciones:

        datos = [
            r
            for r in resultados
            if r["operacion"] == operacion
        ]


        hilos = [
            r["hilos"]
            for r in datos
        ]


        rendimiento = [
            r["ops_s"]
            for r in datos
        ]


        ax.plot(
            hilos,
            rendimiento,
            marker="o",
            markersize=6,
            linewidth=2.4,
            color=COLORES[operacion]
        )


    ax.set_title(
        "Rendimiento según concurrencia",
        fontweight="bold",
        loc="center",
        pad=12
    )


    ax.set_xlabel(
        "Cantidad de hilos"
    )


    ax.set_ylabel(
        "Operaciones por segundo"
    )


    ax.set_xticks(
        HILOS_PRUEBA
    )


    ax.set_ylim(
        bottom=0
    )


    ax.yaxis.set_major_formatter(
        FuncFormatter(
            lambda valor, posicion:
            f"{valor:,.0f}"
        )
    )


    aplicar_estilo(
        ax
    )


    agregar_leyenda(
        ax,
        operaciones
    )


    plt.tight_layout(
        rect=[
            0,
            0.05,
            1,
            1
        ]
    )


    plt.savefig(
        os.path.join(
            CARPETA_GRAFICOS,
            "01_rendimiento_segun_concurrencia.png"
        ),
        dpi=300,
        bbox_inches="tight"
    )


    plt.close()

# Gráfico 2
# Latencia p99 según concurrencia

def grafico_p99(
    resultados
):

    fig, ax = plt.subplots(
        figsize=TAMANO_FIGURA
    )


    operaciones = [
        "Lectura",
        "Escritura",
        "Actualización atómica"
    ]


    posiciones = np.arange(
        len(HILOS_PRUEBA)
    )


    ancho = 0.23


    for indice, operacion in enumerate(
        operaciones
    ):

        datos = [
            r
            for r in resultados
            if r["operacion"] == operacion
        ]


        valores = [
            r["p99_ms"]
            for r in datos
        ]


        desplazamiento = (
            indice - 1
        ) * ancho


        ax.bar(
            posiciones + desplazamiento,
            valores,
            width=ancho,
            color=COLORES[operacion]
        )


    ax.set_title(
        "Latencia p99 según concurrencia",
        fontweight="bold",
        loc="center",
        pad=12
    )


    ax.set_xlabel(
        "Cantidad de hilos"
    )


    ax.set_ylabel(
        "Latencia p99 (ms)"
    )


    ax.set_xticks(
        posiciones
    )


    ax.set_xticklabels(
        HILOS_PRUEBA
    )


    ax.set_ylim(
        bottom=0
    )


    aplicar_estilo(
        ax
    )


    agregar_leyenda(
        ax,
        operaciones
    )


    plt.tight_layout(
        rect=[
            0,
            0.05,
            1,
            1
        ]
    )


    plt.savefig(
        os.path.join(
            CARPETA_GRAFICOS,
            "02_latencia_p99.png"
        ),
        dpi=300,
        bbox_inches="tight"
    )


    plt.close()

# Gráfico 3
# Latencia promedio con 32 hilos

def grafico_latencia_promedio(
    resultados
):

    datos = [
        r
        for r in resultados
        if r["hilos"] == 32
    ]


    operaciones = [
        r["operacion"]
        for r in datos
    ]


    valores = [
        r["latencia_promedio_ms"]
        for r in datos
    ]


    colores = [
        COLORES[operacion]
        for operacion in operaciones
    ]


    fig, ax = plt.subplots(
        figsize=TAMANO_FIGURA
    )


    barras = ax.barh(
        operaciones,
        valores,
        color=colores,
        height=0.52
    )


    ax.set_title(
        "Latencia promedio con 32 hilos",
        fontweight="bold",
        loc="center",
        pad=12
    )


    ax.set_xlabel(
        "Latencia promedio (ms)"
    )


    aplicar_estilo(
        ax,
        eje_grid="x"
    )


    mayor = max(
        valores
    )

    for barra, valor in zip(
        barras,
        valores
    ):

        ax.text(
            valor + mayor * 0.025,
            barra.get_y()
            + barra.get_height() / 2,
            f"{valor:.2f} ms",
            va="center",
            ha="left",
            fontsize=10
        )

    ax.set_xlim(
        0,
        mayor * 1.22
    )

    ax.invert_yaxis()


    plt.tight_layout()


    plt.savefig(
        os.path.join(
            CARPETA_GRAFICOS,
            "03_latencia_promedio_32_hilos.png"
        ),
        dpi=300,
        bbox_inches="tight"
    )


    plt.close()

def ejecutar_metricas():

    limpiar_resultados()

    client = conectar()

    resultados = []

    key_inventario = (
        NAMESPACE,
        "inventario",
        f"evt:{EVENTO}:zona:{ZONA}"
    )

    try:

        client.get(
            key_inventario
        )

    except ex.RecordNotFound:

        print(
            "No se encontró el inventario. "
            "Ejecuta generar_datos.py primero."
        )

        client.close()

        return


    print(
        "Iniciando pruebas de rendimiento..."
    )


    print(
        f"Operaciones por prueba: "
        f"{OPERACIONES_POR_PRUEBA:,}"
    )


    print(
        f"Hilos evaluados: "
        f"{HILOS_PRUEBA}"
    )


    for hilos in HILOS_PRUEBA:

        print(
            f"\nProbando con {hilos} hilos..."
        )


        # Lectura

        operacion = crear_operacion_lectura(
            client
        )


        (
            latencias,
            exitos,
            fallos,
            tiempo
        ) = ejecutar_prueba(
            operacion,
            hilos
        )


        resultado = calcular_metricas(
            "Lectura",
            hilos,
            latencias,
            exitos,
            fallos,
            tiempo
        )


        resultados.append(
            resultado
        )


        mostrar_resultado(
            resultado
        )


        # Escritura

        operacion = crear_operacion_escritura(
            client,
            hilos
        )


        (
            latencias,
            exitos,
            fallos,
            tiempo
        ) = ejecutar_prueba(
            operacion,
            hilos
        )


        resultado = calcular_metricas(
            "Escritura",
            hilos,
            latencias,
            exitos,
            fallos,
            tiempo
        )


        resultados.append(
            resultado
        )


        mostrar_resultado(
            resultado
        )


        # Actualización atómica

        key_cas = preparar_contador_cas(
            client,
            hilos
        )


        operacion = crear_operacion_cas(
            client,
            key_cas
        )


        (
            latencias,
            exitos,
            fallos,
            tiempo
        ) = ejecutar_prueba(
            operacion,
            hilos
        )


        resultado = calcular_metricas(
            "Actualización atómica",
            hilos,
            latencias,
            exitos,
            fallos,
            tiempo
        )


        resultados.append(
            resultado
        )


        mostrar_resultado(
            resultado
        )

        _, _, contador = client.get(
            key_cas
        )


        print(
            f"Contador final: "
            f"{contador['contador']:,}"
        )


    grafico_rendimiento(
        resultados
    )


    grafico_p99(
        resultados
    )


    grafico_latencia_promedio(
        resultados
    )


    client.close()


    print(
        "\nListo. Se generaron los 3 gráficos finales:"
    )

    print(
        "1. Rendimiento según concurrencia"
    )

    print(
        "2. Latencia p99 según concurrencia"
    )

    print(
        "3. Latencia promedio con 32 hilos"
    )

if __name__ == "__main__":

    ejecutar_metricas()