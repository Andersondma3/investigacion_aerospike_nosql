import os
import time
import numpy as np
from concurrent.futures import ThreadPoolExecutor

import aerospike
from aerospike import exception as ex

try:
    import reservas
except ImportError:
    reservas = None


# Configuración de Aerospike

_EN_DOCKER = os.path.exists("/.dockerenv")

HOST = os.getenv(
    "AEROSPIKE_HOST",
    "aerospike" if _EN_DOCKER else "127.0.0.1"
)

PORT = int(os.getenv("AEROSPIKE_PORT", "3000"))
NAMESPACE = os.getenv("AEROSPIKE_NAMESPACE", "test")

SET_INVENTARIO = "inventario"
SET_RESERVAS = "reservas"
SET_CONTROLES = "controles"


# Configuración de la prueba

# Requisitos mínimos
# 50 000 usuarios
# 100 000 intentos de reserva

TOTAL_USUARIOS = 50000
TOTAL_PETICIONES = 100000

# Dejamos 32 hilos para hacer varias solicitudes al mismo tiempo

CONCURRENCIA_HILOS = 32

# Zona que vamos a usar para la prueba

EVT_ID = 1
ZONA_OBJETIVO = "VIP"


config = {
    "hosts": [(HOST, PORT)]
}


# Conexión con Aerospike

def conectar():

    politica = {
        "key": aerospike.POLICY_KEY_SEND
    }

    cliente = aerospike.client(config).connect()

    return cliente, politica


# Hacemos un intento de reserva

def ejecutar_transaccion_reserva(cliente, intento_id):

    inicio = time.perf_counter()
    exito = False

    # Cada usuario hace dos intentos

    usuario_id = ((intento_id - 1) % TOTAL_USUARIOS) + 1

    clave_inventario = (
        NAMESPACE,
        SET_INVENTARIO,
        f"evt:{EVT_ID}:zona:{ZONA_OBJETIVO}"
    )

    try:

        # Usamos la lógica que ya tenemos en reservas.py

        if reservas and hasattr(
            reservas,
            "crear_reserva_temporal"
        ):

            resultado = reservas.crear_reserva_temporal(
                cliente,
                clave_inventario,
                f"usr_bench_{usuario_id}",
                intento_id
            )

            exito = bool(resultado)

        else:

            # Si no encuentra reservas.py usamos CAS directamente

            for _ in range(10):

                _, meta, bins = cliente.get(
                    clave_inventario
                )

                stock_actual = bins.get(
                    "stock",
                    0
                )

                if stock_actual < 1:
                    exito = False
                    break

                politica_cas = {
                    "gen": aerospike.POLICY_GEN_EQ
                }

                meta_cas = {
                    "gen": meta["gen"]
                }

                try:

                    cliente.put(
                        clave_inventario,
                        {
                            "stock": stock_actual - 1
                        },
                        meta=meta_cas,
                        policy=politica_cas
                    )

                    exito = True
                    break

                except ex.RecordGenerationError:
                    continue

    except Exception:
        exito = False

    latencia_ms = (
        time.perf_counter() - inicio
    ) * 1000.0

    return latencia_ms, exito


# Verificamos que el inventario quede bien

def verificar_inventario(
    cliente,
    reservas_creadas
):

    clave_inventario = (
        NAMESPACE,
        SET_INVENTARIO,
        f"evt:{EVT_ID}:zona:{ZONA_OBJETIVO}"
    )

    _, _, bins = cliente.get(
        clave_inventario
    )

    stock_final = bins.get(
        "stock",
        0
    )

    capacidad_inicial = bins.get(
        "capacidad",
        0
    )

    stock_esperado = (
        capacidad_inicial - reservas_creadas
    )

    print(
        "\n--- VERIFICACIÓN DEL INVENTARIO ---"
    )

    print(
        f"Zona evaluada:               "
        f"evt:{EVT_ID}:zona:{ZONA_OBJETIVO}"
    )

    print(
        f"Capacidad inicial:           "
        f"{capacidad_inicial:,}"
    )

    print(
        f"Reservas creadas:            "
        f"{reservas_creadas:,}"
    )

    print(
        f"Stock esperado:              "
        f"{stock_esperado:,}"
    )

    print(
        f"Stock final:                 "
        f"{stock_final:,}"
    )

    if (
        stock_final >= 0
        and stock_final == stock_esperado
    ):

        print(
            "Resultado: inventario consistente, "
            "sin sobreventa."
        )

    else:

        print(
            "Resultado: se encontró una "
            "inconsistencia en el inventario."
        )


# Iniciamos la prueba

def iniciar_benchmark():

    # Quitamos los prints de reservas.py para que no salgan miles de mensajes

    if reservas:
        reservas.print = (
            lambda *args, **kwargs: None
        )

    print(
        "=================================================="
    )

    print(
        "EntradaFlash CR - Prueba de carga y concurrencia"
    )

    print(
        "=================================================="
    )

    print(
        f"Servidor:                    "
        f"{HOST}:{PORT}"
    )

    print(
        f"Namespace:                   "
        f"{NAMESPACE}"
    )

    print(
        f"Usuarios simulados:          "
        f"{TOTAL_USUARIOS:,}"
    )

    print(
        f"Intentos de reserva:         "
        f"{TOTAL_PETICIONES:,}"
    )

    print(
        f"Intentos por usuario:        "
        f"{TOTAL_PETICIONES / TOTAL_USUARIOS:.1f}"
    )

    print(
        f"Hilos concurrentes:          "
        f"{CONCURRENCIA_HILOS}"
    )

    print(
        "==================================================\n"
    )


    cliente, _ = conectar()

    # Guardamos las latencias y contamos los resultados

    latencias = []

    reservas_creadas = 0
    intentos_rechazados = 0

    inicio_prueba = time.perf_counter()


    # Ejecutamos los intentos al mismo tiempo

    with ThreadPoolExecutor(
        max_workers=CONCURRENCIA_HILOS
    ) as executor:

        futuros = [

            executor.submit(
                ejecutar_transaccion_reserva,
                cliente,
                intento_id
            )

            for intento_id in range(
                1,
                TOTAL_PETICIONES + 1
            )
        ]

        for futuro in futuros:

            latencia, exito = futuro.result()

            latencias.append(
                latencia
            )

            if exito:
                reservas_creadas += 1

            else:
                intentos_rechazados += 1


    # Calculamos los resultados

    duracion_total = (
        time.perf_counter()
        - inicio_prueba
    )

    solicitudes_por_segundo = (
        TOTAL_PETICIONES
        / duracion_total
    )

    p50 = np.percentile(
        latencias,
        50
    )

    p95 = np.percentile(
        latencias,
        95
    )

    p99 = np.percentile(
        latencias,
        99
    )

    latencia_promedio = np.mean(
        latencias
    )


    # Mostramos los resultados

    print(
        "\n================ RESULTADOS DE LA PRUEBA ================"
    )

    print(
        f"Usuarios simulados:          "
        f"{TOTAL_USUARIOS:,}"
    )

    print(
        f"Intentos de reserva:         "
        f"{TOTAL_PETICIONES:,}"
    )

    print(
        f"Hilos concurrentes:          "
        f"{CONCURRENCIA_HILOS}"
    )

    print()

    print(
        f"Tiempo total:                "
        f"{duracion_total:.2f} s"
    )

    print(
        f"Solicitudes por segundo:     "
        f"{solicitudes_por_segundo:.2f}"
    )

    print(
        f"Reservas creadas:            "
        f"{reservas_creadas:,}"
    )

    print(
        f"Intentos rechazados:         "
        f"{intentos_rechazados:,}"
    )

    print()

    print(
        f"Latencia promedio:           "
        f"{latencia_promedio:.3f} ms"
    )

    print(
        f"Latencia p50:                "
        f"{p50:.3f} ms"
    )

    print(
        f"Latencia p95:                "
        f"{p95:.3f} ms"
    )

    print(
        f"Latencia p99:                "
        f"{p99:.3f} ms"
    )


    # Verificamos el inventario al final

    verificar_inventario(
        cliente,
        reservas_creadas
    )

    cliente.close()


# Ejecutamos

if __name__ == "__main__":
    iniciar_benchmark()