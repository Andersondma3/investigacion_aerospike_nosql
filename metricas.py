import aerospike
from aerospike import exception as ex
import os
import time
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed


# Configuracion de Aerospike

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


# Datos usados para las pruebas

HILOS_PRUEBA = [1, 8, 16, 32, 64]
OPERACIONES_POR_PRUEBA = 5000

EVENTO = 1
ZONA = "VIP"


def conectar():
    return aerospike.client(config).connect()


# Calcula las metricas de cada prueba

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


# Ejecuta una prueba usando varios hilos

def ejecutar_prueba(funcion, hilos):
    latencias = []

    exitos = 0
    fallos = 0

    inicio_total = time.perf_counter()

    with ThreadPoolExecutor(max_workers=hilos) as executor:

        futuros = [
            executor.submit(funcion, numero)
            for numero in range(OPERACIONES_POR_PRUEBA)
        ]

        for futuro in as_completed(futuros):

            latencia, exito = futuro.result()

            if exito:
                exitos += 1
                latencias.append(latencia)

            else:
                fallos += 1

    tiempo_total = time.perf_counter() - inicio_total

    return latencias, exitos, fallos, tiempo_total


# Prueba de lectura

def crear_operacion_lectura(client):

    key = (
        NAMESPACE,
        "inventario",
        f"evt:{EVENTO}:zona:{ZONA}"
    )

    def leer(numero):

        inicio = time.perf_counter()

        try:
            client.get(key)
            exito = True

        except ex.AerospikeError:
            exito = False

        latencia = (
            time.perf_counter() - inicio
        ) * 1000

        return latencia, exito

    return leer


# Prueba de escritura

def crear_operacion_escritura(client, hilos):

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
            time.perf_counter() - inicio
        ) * 1000

        return latencia, exito

    return escribir


# Crea el contador usado para la prueba atomica

def preparar_contador_cas(client, hilos):

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


# Actualizacion atomica usando la generacion del registro

def crear_operacion_cas(client, key):

    def actualizar(numero):

        inicio = time.perf_counter()

        reintentos = 0
        exito = False

        while reintentos < 100:

            try:
                _, meta, datos = client.get(key)

                contador_actual = datos.get(
                    "contador",
                    0
                )

                datos["contador"] = contador_actual + 1

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
                    time.sleep(0.0005)

            except ex.AerospikeError:
                break

        latencia = (
            time.perf_counter() - inicio
        ) * 1000

        return latencia, exito

    return actualizar


# Muestra las metricas de una prueba

def mostrar_resultado(resultado):

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


# Al final muestra todas las pruebas juntas

def mostrar_resumen(resultados):

    print("\nResumen general de metricas\n")

    print(
        f"{'Operacion':<23}"
        f"{'Hilos':>7}"
        f"{'Correctas':>11}"
        f"{'Fallos':>9}"
        f"{'Ops/s':>12}"
        f"{'Prom ms':>11}"
        f"{'p50 ms':>10}"
        f"{'p95 ms':>10}"
        f"{'p99 ms':>10}"
    )

    for resultado in resultados:

        print(
            f"{resultado['operacion']:<23}"
            f"{resultado['hilos']:>7}"
            f"{resultado['exitos']:>11,}"
            f"{resultado['fallos']:>9,}"
            f"{resultado['ops_s']:>12.2f}"
            f"{resultado['latencia_promedio_ms']:>11.3f}"
            f"{resultado['p50_ms']:>10.3f}"
            f"{resultado['p95_ms']:>10.3f}"
            f"{resultado['p99_ms']:>10.3f}"
        )


def ejecutar_metricas():

    client = conectar()
    resultados = []

    key_inventario = (
        NAMESPACE,
        "inventario",
        f"evt:{EVENTO}:zona:{ZONA}"
    )

    # Primero revisamos que existan los datos

    try:
        client.get(key_inventario)

    except ex.RecordNotFound:

        print(
            "No se encontro el inventario. "
            "Ejecuta generar_datos.py primero."
        )

        client.close()
        return

    print("\nIniciando pruebas de rendimiento...")

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

        latencias, exitos, fallos, tiempo = ejecutar_prueba(
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

        resultados.append(resultado)

        mostrar_resultado(
            resultado
        )

        # Escritura

        operacion = crear_operacion_escritura(
            client,
            hilos
        )

        latencias, exitos, fallos, tiempo = ejecutar_prueba(
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

        resultados.append(resultado)

        mostrar_resultado(
            resultado
        )

        # Actualizacion atomica

        key_cas = preparar_contador_cas(
            client,
            hilos
        )

        operacion = crear_operacion_cas(
            client,
            key_cas
        )

        latencias, exitos, fallos, tiempo = ejecutar_prueba(
            operacion,
            hilos
        )

        resultado = calcular_metricas(
            "Actualizacion atomica",
            hilos,
            latencias,
            exitos,
            fallos,
            tiempo
        )

        resultados.append(resultado)

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

    client.close()

    mostrar_resumen(
        resultados
    )

    print(
        "\nPruebas de rendimiento finalizadas."
    )


if __name__ == "__main__":
    ejecutar_metricas()