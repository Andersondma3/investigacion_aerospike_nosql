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
TOTAL_PETICIONES = 5_000
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
    