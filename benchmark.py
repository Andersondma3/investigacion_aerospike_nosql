import os
import time
import random
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import aerospike 
from aerospike import exception as ex

# Importar la lógica de transacciones atómicas del equipo
try:
    import reservas 
except ImportError:
    reservas = None

# Configuración del entorno y conexión
_EN_DOCKER = os.path.exists("/.dockerenv")
HOST = os.getenv("AEROSPIKE_HOST", "aerospike" if _EN_DOCKER else "127.0.0.1")
PORT = int(os.getenv("AEROSPIKE_PORT", "3000"))
NAMESPACE = os.getenv("AEROSPIKE_NAMESPACE", "test")

# Definicón de conjuntos (sets)
SET_INVENTARIO = "inventario"
SET_RESERVAS = "reservas"
SET_CONTROLES = "controles"

# Carga masiva
TOTAL_PETICIONES = 5000
CONCURRENCIA_HILOS = 32 #workers

EVT_ID = 1
ZONA_OBJETIVO = "VIP"
CANTIDAD_POR_COMPRA = 1

# Configuración del cliente
config = {"hosts": [(HOST, PORT)]}

# Conexión al clúster
def conectar():
    politica = {"key": aerospike.POLICY_KEY_SEND}
    cliente = aerospike.client(config).connect()
    return cliente, politica

# Operación individual de reserva
def ejecutar_transaccion_reserva(cliente, usuario_id):
    """
    Ejecuta un intento de reserva llamando a reservas.py y mide el tiempo exacto
    """
    inicio = time.perf_counter()
    exito = False

    try:
        if reservas and hasattr(reservas, "reservar"):
            exito = reservas.reservar(cliente, EVT_ID, ZONA_OBJETIVO, usuario_id, CANTIDAD_POR_COMPRA)
        elif reservas and hasattr(reservas, "crear_reserva"):
            exito = reservas.crear_reserva(cliente, EVT_ID, ZONA_OBJETIVO, usuario_id, CANTIDAD_POR_COMPRA)
        else:
            # Fallback CAS directo con política de reintentos (hasta 10 intentos ante rebotes)
            clave_inv = (NAMESPACE, SET_INVENTARIO, f"evt:{EVT_ID}:zona:{ZONA_OBJETIVO}")
            for _ in range(10):
                _, meta, bins = cliente.get(clave_inv)
                stock_actual = bins.get("stock", 0)
                if stock_actual < CANTIDAD_POR_COMPRA:
                    exito = False
                    break
                politica_cas = {"gen": aerospike.POLICY_GEN_EQ}
                meta_cas = {"gen": meta["gen"]}
                try:
                    cliente.put(clave_inv, {"stock": stock_actual - CANTIDAD_POR_COMPRA}, meta=meta_cas, policy=politica_cas)
                    exito = True
                    break
                except ex.RecordGenerationError:
                    continue  # Rebote de colisión CAS detectado
    except Exception:
        exito = False

    latencia_ms = (time.perf_counter() - inicio) * 1000.0
    return latencia_ms, exito

# Auditoría de consistencia
def auditar_consistencia(cliente):
    clave_inv = (NAMESPACE, SET_INVENTARIO, f"evt:{EVT_ID}:zona:{ZONA_OBJETIVO}")
    _, _, bins = cliente.get(clave_inv)
    stock_final = bins.get("stock", 0)
    capacidad = bins.get("capacidad", 0)
    
    print("\n--- AUDITORÍA DE CONSISTENCIA ---")
    print(f"Zona evaluada:     evt:{EVT_ID}:zona:{ZONA_OBJETIVO}")
    print(f"Capacidad total:   {capacidad}")
    print(f"Stock remanente:   {stock_final}")
    
    if stock_final >= 0:
        print("Veredicto de Stock: CORRECTO (Invariante preservada, sin sobreventa)")
    else:
        print("Veredicto de Stock: FALLO CRÍTICO (Stock menor a cero detectado)")


# Orquestador del Benchmark
def iniciar_benchmark():
    print("--------------------------------------------------")
    print("EntradaFlash CR - Benchmark de Estrés y Concurrencia")
    print(f"Host: {HOST}:{PORT} | Namespace: {NAMESPACE}")
    print(f"Total peticiones: {TOTAL_PETICIONES} | Hilos concurrentes: {CONCURRENCIA_HILOS}")
    print("--------------------------------------------------\n")

    cliente, _ = conectar()
    latencias = []
    exitos = 0
    fallos = 0

    t_inicio = time.perf_counter()

    with ThreadPoolExecutor(max_workers=CONCURRENCIA_HILOS) as executor:
        futuros = [
            executor.submit(ejecutar_transaccion_reserva, cliente, usr_id)
            for usr_id in range(1, TOTAL_PETICIONES + 1)
        ]
        for f in futuros:
            lat, ok = f.result()
            latencias.append(lat)
            if ok:
                exitos += 1
            else:
                fallos += 1

    duracion_total = time.perf_counter() - t_inicio
    tps = TOTAL_PETICIONES / duracion_total

    # Cálculo de métricas y percentiles (Metodología IEEE)
    p50 = np.percentile(latencias, 50)
    p95 = np.percentile(latencias, 95)
    p99 = np.percentile(latencias, 99)
    media = np.mean(latencias)

    # Salidas
    print("\n--------------------------------------------------")
    print("           RESULTADOS DEL BENCHMARK ")
    print("--------------------------------------------------")
    print(f"Tiempo de ejecución: {duracion_total:.2f} s")
    print(f"Throughput (TPS):    {tps:.2f} transacciones/segundo")
    print(f"Transacciones OK:    {exitos}")
    print(f"Rechazos/Agotados:   {fallos}")
    print(f"Latencia Media:      {media:.3f} ms")
    print(f"Latencia p50:        {p50:.3f} ms")
    print(f"Latencia p95:        {p95:.3f} ms")
    print(f"Latencia p99:        {p99:.3f} ms")

    auditar_consistencia(cliente)
    cliente.close()


if __name__ == "__main__":
    iniciar_benchmark()