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

_EN_DOCKER = os.path.exists("/.dockerenv")
HOST = os.getenv("AEROSPIKE_HOST", "aerospike" if _EN_DOCKER else "127.0.0.1")
PORT = int(os.getenv("AEROSPIKE_PORT", "3000"))
NAMESPACE = os.getenv("AEROSPIKE_NAMESPACE", "test")

SET_INVENTARIO = "inventario"
SET_RESERVAS = "reservas"
SET_CONTROLES = "controles"

TOTAL_PETICIONES = 5000
CONCURRENCIA_HILOS = 32

EVT_ID = 1
ZONA_OBJETIVO = "VIP"

config = {"hosts": [(HOST, PORT)]}

def conectar():
    politica = {"key": aerospike.POLICY_KEY_SEND}
    cliente = aerospike.client(config).connect()
    return cliente, politica

def ejecutar_transaccion_reserva(cliente, usuario_id):
    inicio = time.perf_counter()
    exito = False
    clave_inv = (NAMESPACE, SET_INVENTARIO, f"evt:{EVT_ID}:zona:{ZONA_OBJETIVO}")

    try:
        # Enlace directo a la lógica unificada de Anderson y Samuel
        if reservas and hasattr(reservas, "crear_reserva_temporal"):
            resultado = reservas.crear_reserva_temporal(cliente, clave_inv, f"usr_bench_{usuario_id}", usuario_id)
            exito = bool(resultado)
        else:
            # Fallback CAS directo
            for _ in range(10):
                _, meta, bins = cliente.get(clave_inv)
                stock_actual = bins.get("stock", 0)
                if stock_actual < 1:
                    exito = False
                    break
                politica_cas = {"gen": aerospike.POLICY_GEN_EQ}
                meta_cas = {"gen": meta["gen"]}
                try:
                    cliente.put(clave_inv, {"stock": stock_actual - 1}, meta=meta_cas, policy=politica_cas)
                    exito = True
                    break
                except ex.RecordGenerationError:
                    continue
    except Exception:
        exito = False

    latencia_ms = (time.perf_counter() - inicio) * 1000.0
    return latencia_ms, exito

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

def iniciar_benchmark():
    # Desactivar temporalmente los prints de reservas.py para no saturar la consola en la prueba de estrés
    if reservas:
        import sys
        reservas.print = lambda *args, **kwargs: None

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

    p50 = np.percentile(latencias, 50)
    p95 = np.percentile(latencias, 95)
    p99 = np.percentile(latencias, 99)
    media = np.mean(latencias)

    print("--------------------------------------------------")
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