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

