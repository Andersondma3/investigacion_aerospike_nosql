import aerospike
from aerospike import exception as ex
import threading
import time
import os


# Configuración de conexión adaptada para funcionar igual que el script de Kelsy

# ==========================================
# CONFIGURACIÓN (Anderson - Entorno Docker)
# ==========================================

_EN_DOCKER = os.path.exists("/.dockerenv")
HOST = os.getenv("AEROSPIKE_HOST", "aerospike" if _EN_DOCKER else "127.0.0.1")
PORT = int(os.getenv("AEROSPIKE_PORT", "3000"))
config = {"hosts": [(HOST, PORT)]}

# (Samuel - Configuración de expiración para pruebas)
TTL_RESERVA = 10  # Segundos de vida de la reserva

# ==========================================
# MÓDULO 1: RESERVAS ATÓMICAS (Anderson)
# ==========================================
def reservar_entrada_cas(client, key, id_usuario):
    max_reintentos = 10
    intentos = 0
    
    while intentos < max_reintentos:
        try:

            # 1. Leer el registro y su Meta (que contiene la 'generación' o versión)
            key_tuple, meta, bins = client.get(key)
            stock_actual = bins.get('stock', 0)
            
            # 2. Validar inventario localmente
            if stock_actual < 1:
                print(f"[{id_usuario}] RECHAZADO: Boletos agotados.")
                return False
            
            # 3. Preparar los nuevos datos
            bins['stock'] = stock_actual - 1
            
            # 4. Intentar guardar con política de Generación Exacta (CAS)
            politica = {'gen': aerospike.POLICY_GEN_EQ}
            
            # Si alguien modificó el registro desde nuestro client.get(), esto fallará
            client.put(key, bins, policy=politica, meta=meta)
            
            print(f"[{id_usuario}] ÉXITO: Entrada asegurada. Stock restante: {bins['stock']} (Logrado en intento {intentos+1})")
            return True
            
        except ex.RecordGenerationError:
            # Ocurrió una colisión: otro hilo nos ganó. Reintentamos el ciclo.
            intentos += 1
            time.sleep(0.01) # Breve pausa para descongestionar el tráfico
            
        except ex.RecordNotFound:
            print(f"[{id_usuario}] ERROR: El evento no existe. ¿Ejecutaste generar_datos.py primero?")
            return False
            
        except ex.AerospikeError as e:
            print(f"[{id_usuario}] ERROR de Base de datos: {e}")
            return False
            
    print(f"[{id_usuario}] FALLO: Demasiada concurrencia tras {max_reintentos} intentos. Intente de nuevo.")
    return False

# ==========================================
# MÓDULO 2: GESTIÓN DE EXPIRACIÓN TTL (Samuel)
# ==========================================
def crear_reserva_temporal(client, key_inventario, id_usuario, id_reserva):
    # 1. Descontar inventario usando CAS (Lógica Anderson)
    exito = reservar_entrada_cas(client, key_inventario, id_usuario)
    if not exito:
        print(f"[{id_usuario}] RECHAZADO: Sin stock.")
        return False

    # 2. Clave de la reserva temporal
    key_reserva = ("test", "reservas", f"reserva:{id_reserva}")

    # 3. Datos de la reserva y TTL
    reserva = {"usuario": id_usuario, "cantidad": 1, "estado": "pendiente"}
    client.put(key_reserva, reserva, meta={"ttl": TTL_RESERVA})

    # 4. Registro de control (No expira, sirve para auditoría)
    key_control = ("test", "controles", f"control:{id_reserva}")
    control = {
        "reserva_id": f"reserva:{id_reserva}",
        "cantidad": 1,
        "inventario": key_inventario[2],
        "estado": "pendiente"
    }
    client.put(key_control, control)

    print(f"[{id_usuario}] ÉXITO: Reserva temporal creada. Expira en {TTL_RESERVA}s.")
    return key_reserva, key_control

def procesar_expiraciones(client, reservas_creadas):
    print("\n========== PROCESANDO EXPIRACIONES (TTL) ==========")
    
    for key_reserva, key_control in reservas_creadas:
        # 1. Comprobar si la reserva fue eliminada por Aerospike (Expiró)
        try:
            client.get(key_reserva)
            print(f"-> {key_reserva[2]} todavía activa (Usuario pagó o está en tiempo).")
            continue
        except ex.RecordNotFound:
            print(f"-> {key_reserva[2]} EXPIRÓ. Iniciando devolución de stock...")

        # 2. Obtener control y verificar estado
        try:
            _, meta_control, control = client.get(key_control)
        except ex.RecordNotFound:
            continue
            
        if control["estado"] != "pendiente":
            continue

        key_inventario = ("test", "inventario", control["inventario"])
        cantidad_a_devolver = control["cantidad"]

        # 3. Devolver la entrada al inventario (Con candado CAS robusto aplicado)
        intentos = 0
        devuelto = False
        while intentos < 10 and not devuelto:
            try:
                _, meta_inventario, inventario = client.get(key_inventario)
                inventario["stock"] += cantidad_a_devolver
                
                client.put(
                    key_inventario, 
                    inventario, 
                    policy={"gen": aerospike.POLICY_GEN_EQ}, 
                    meta=meta_inventario
                )
                print(f"   [+] Inventario recuperado: ahora hay {inventario['stock']} boletos.")
                devuelto = True
            except ex.RecordGenerationError:
                intentos += 1
                time.sleep(0.01)

        # 4. Marcar control como procesado
        if devuelto:
            control["estado"] = "procesada_y_devuelta"
            try:
                client.put(key_control, control, policy={"gen": aerospike.POLICY_GEN_EQ}, meta=meta_control)
            except ex.RecordGenerationError:
                pass

# ==========================================
# MÓDULO 3: SIMULADOR DE CONCURRENCIA
# ==========================================

def simular_concurrencia():
    try:
        client = aerospike.client(config).connect()
    except Exception as e:
        print(f"Error de conexión: {e}")
        return
    # Apuntamos a un evento real generado por el script de Kelsy
    # Evento 1: Festival Rock Tico, Zona: VIP (Inicia con 800 boletos)
    key = ("test", "inventario", "evt:1:zona:VIP")
    
    try:
        # Leemos el stock inicial real de la base de datos
        _, _, record_inicial = client.get(key)
        stock_inicial = record_inicial.get("stock", 0)
        print(f"[SETUP] Conectado al evento. Stock inicial detectado: {stock_inicial}")
    except ex.RecordNotFound:
        print("[ERROR] No se encontró el evento. Asegúrate de ejecutar el script de Kelsy (generar_datos.py) antes de correr esta prueba.")
        client.close()
        return
    
    total_compradores = 25
    resultados = {"exitos": 0, "fallos": 0}
    lock = threading.Lock()

    def tarea_comprador(user_id):
        exito = reservar_entrada_cas(client, key, user_id)
        with lock:
            if exito:
                resultados["exitos"] += 1
    # Apuntar a la base de datos real generada por Kelsy
    key = ("test", "inventario", "evt:1:zona:VIP")
    
    try:
        _, _, record_inicial = client.get(key)
        stock_inicial = record_inicial.get("stock", 0)
        print(f"[SETUP] Conectado al evento. Stock detectado: {stock_inicial}")
    except ex.RecordNotFound:
        print("ERROR: Ejecuta generar_datos.py primero.")
        client.close()
        return

    total_compradores = 25
    resultados = {"exitos": 0, "fallos": 0}
    lock = threading.Lock()
    reservas_creadas = []

    def tarea_comprador(user_id, numero_reserva):
        resultado = crear_reserva_temporal(client, key, user_id, numero_reserva)
        with lock:
            if resultado:
                resultados["exitos"] += 1
                reservas_creadas.append(resultado)
            else:
                resultados["fallos"] += 1

    hilos = []

    print(f"\n--- Iniciando ráfaga con {total_compradores} compradores concurrentes ---")
    inicio = time.time()
    
    for i in range(total_compradores):
        t = threading.Thread(target=tarea_comprador, args=(f"Usuario_{i+1:02d}",))

    print(f"\n--- Ráfaga de {total_compradores} compradores concurrentes ---")
    
    for i in range(total_compradores):
        t = threading.Thread(target=tarea_comprador, args=(f"Usuario_{i+1:02d}", i+1))
        hilos.append(t)
        t.start()

    for t in hilos:
        t.join()
        
    fin = time.time()

    # Validación final del inventario
    _, _, record_final = client.get(key)
    client.close()

    print("\n================ RESULTADOS FINALES ================")
    print(f"Tiempo total de simulación: {(fin - inicio)*1000:.2f} ms")
    print(f"Reservas exitosas: {resultados['exitos']}")
    print(f"Reservas rechazadas: {resultados['fallos']}")
    print(f"Stock inicial: {stock_inicial} | Stock final en BD: {record_final['stock']}")
    
    # Nueva lógica de validación matemática adaptada a cualquier volumen de datos
    if record_final["stock"] == (stock_inicial - resultados["exitos"]):
        print("VEREDICTO: Consistencia perfecta mediante Bloqueo Optimista (CAS). Matemáticas exactas.")
    else:
        print("ALERTA: Se detectó una inconsistencia en los datos.")

if __name__ == "__main__":
    simular_concurrencia()

"""
Carga sintetica y reproducible de EntradaFlash CR en Aerospike.
"""

import argparse
import os
import random
import sys
import time
import warnings

# Ocultar el aviso de depreciación del TTL para limpiar la consola
warnings.filterwarnings("ignore", category=DeprecationWarning)

# -------------------------------------
# Conexion
# -------------------------------------

_EN_DOCKER = os.path.exists("/.dockerenv")
HOST = os.getenv("AEROSPIKE_HOST", "aerospike" if _EN_DOCKER else "127.0.0.1")
PORT = int(os.getenv("AEROSPIKE_PORT", "3000"))
NAMESPACE = os.getenv("AEROSPIKE_NAMESPACE", "test")

# -------------------------------------
# Sets
# --------------------------------------
SET_EVENTOS = "eventos"
SET_INVENTARIO = "inventario"  # lo lee reservas.py (algoritmo CAS)
SET_USUARIOS = "usuarios"
SET_BOLETOS = "boletos"
SET_RESERVAS = "reservas"      # lo usa reservas.py / expiracion_ttl.py
SET_SESIONES = "sesiones"      # carrito por usuario
SET_RATELIMIT = "ratelimit"    # control de intentos por usuario

# --------------------------------------
# Bins 
# --------------------------------------
BIN_EVT_ID = "evt_id"
BIN_EVENTO = "evento"
BIN_ZONA = "zona"
BIN_STOCK = "stock"
BIN_CAPACIDAD = "capacidad"
BIN_PRECIO = "precio"
BIN_NOMBRE = "nombre"
BIN_FECHA = "fecha"
BIN_LUGAR = "lugar"
BIN_ZONAS = "zonas"
BIN_USR_ID = "usr_id"
BIN_EMAIL = "email"
BIN_NUMERO = "numero"
BIN_ESTADO = "estado"

MAX_LARGO_BIN = 14

def validar_bins():
    errores = []
    for nombre, valor in globals().items():
        if not nombre.startswith("BIN_"):
            continue
        if len(valor) > MAX_LARGO_BIN:
            errores.append(f"{nombre}='{valor}' tiene {len(valor)} caracteres")
        if not valor.isascii():
            errores.append(f"{nombre}='{valor}' contiene caracteres no ASCII")
    if errores:
        raise ValueError("Nombres de bins invalidos:\n  " + "\n  ".join(errores))

# -------------------------------------
# Claves primarias
# -------------------------------------
def clave_evento(evt_id):
    return f"evt:{evt_id}"

def clave_zona(evt_id, zona):
    return f"evt:{evt_id}:zona:{zona}"

def clave_usuario(usr_id):
    return f"usr:{usr_id:05d}"

def clave_boleto(evt_id, zona, numero):
    return f"evt:{evt_id}:zona:{zona}:bol:{numero:05d}"

def key(set_name, clave):
    return (NAMESPACE, set_name, clave)

# ------------------------------------
# Datos de los eventos 
# ------------------------------------
EVENTOS = [
    {
        "id": 1,
        "nombre": "Festival Rock Tico",
        "fecha": "2026-11-14",
        "lugar": "Estadio Nacional",
        "zonas": {"VIP": (800, 95000), "Platea": (2400, 55000), "General": (4800, 30000)},
    },
    {
        "id": 2,
        "nombre": "Final del Campeonato Nacional",
        "fecha": "2026-12-20",
        "lugar": "Estadio de Futbol",
        "zonas": {"Palco": (500, 60000), "Sombra": (3000, 25000), "Sol": (4000, 15000)},
    },
    {
        "id": 3,
        "nombre": "Gala Sinfonica",
        "fecha": "2026-10-30",
        "lugar": "Teatro Nacional",
        "zonas": {"Palco": (200, 40000), "Platea": (600, 28000), "Galeria": (400, 12000)},
    },
    {
        "id": 4,
        "nombre": "Festival Electronico",
        "fecha": "2027-01-17",
        "lugar": "Parque La Sabana",
        "zonas": {"VIP": (1000, 80000), "General": (3500, 35000)},
    },
    {
        "id": 5,
        "nombre": "Noche de Comedia",
        "fecha": "2026-11-28",
        "lugar": "Auditorio Central",
        "zonas": {"VIP": (300, 30000), "Platea": (1200, 20000), "General": (1500, 12000)},
    },
]

MINIMO_ENTRADAS = 20_000

NOMBRES = ["Ana", "Luis", "Maria", "Jose", "Sofia", "Carlos", "Valeria", "Diego",
           "Fernanda", "Andres", "Camila", "Jorge", "Daniela", "Pablo", "Lucia"]
APELLIDOS = ["Mora", "Rodriguez", "Jimenez", "Vargas", "Rojas", "Solano",
             "Chaves", "Castro", "Araya", "Quesada", "Alvarado", "Brenes"]

# ------------------------------------
# Construccion de registros
# ------------------------------------
def registros_eventos():
    for ev in EVENTOS:
        bins = {
            BIN_EVT_ID: ev["id"],
            BIN_NOMBRE: ev["nombre"],
            BIN_FECHA: ev["fecha"],
            BIN_LUGAR: ev["lugar"],
            BIN_ZONAS: list(ev["zonas"].keys()),
        }
        yield key(SET_EVENTOS, clave_evento(ev["id"])), bins

def registros_inventario():
    for ev in EVENTOS:
        for zona, (capacidad, precio) in ev["zonas"].items():
            bins = {
                BIN_EVT_ID: ev["id"],
                BIN_EVENTO: ev["nombre"],
                BIN_ZONA: zona,
                BIN_STOCK: capacidad,
                BIN_CAPACIDAD: capacidad,
                BIN_PRECIO: precio,
            }
            yield key(SET_INVENTARIO, clave_zona(ev["id"], zona)), bins

def registros_boletos():
    for ev in EVENTOS:
        for zona, (capacidad, _precio) in ev["zonas"].items():
            for numero in range(1, capacidad + 1):
                bins = {
                    BIN_EVT_ID: ev["id"],
                    BIN_ZONA: zona,
                    BIN_NUMERO: numero,
                    BIN_ESTADO: "disponible",
                }
                clave = clave_boleto(ev["id"], zona, numero)
                yield key(SET_BOLETOS, clave), bins

def registros_usuarios(cantidad, rng):
    for usr_id in range(1, cantidad + 1):
        nombre = f"{rng.choice(NOMBRES)} {rng.choice(APELLIDOS)}"
        bins = {
            BIN_USR_ID: usr_id,
            BIN_NOMBRE: nombre,
            BIN_EMAIL: f"usuario{usr_id:05d}@ejemplo.cr",
        }
        yield key(SET_USUARIOS, clave_usuario(usr_id)), bins

def total_entradas():
    return sum(cap for ev in EVENTOS for cap, _ in ev["zonas"].values())

# ------------------------------------
# Escritura en Aerospike e Interfaz Visual
# -------------------------------------

def conectar():
    import aerospike  
    config = {"hosts": [(HOST, PORT)]}
    try:
        cliente = aerospike.client(config).connect()
    except Exception as exc:  # noqa: BLE001
        sys.exit(f"No se pudo conectar a Aerospike en {HOST}:{PORT} -> {exc}")
    politica = {"key": aerospike.POLICY_KEY_SEND}
    meta = {"ttl": getattr(aerospike, "TTL_NEVER_EXPIRE", -1)}
    return cliente, politica, meta

def escribir(cliente, politica, meta, registros, etiqueta, cada=5000):
    inicio = time.perf_counter()
    n = 0
    for llave, bins in registros:
        cliente.put(llave, bins, meta=meta, policy=politica)
        n += 1
        if n % cada == 0:
            print(f"  {etiqueta}: {n} registros...")
    seg = time.perf_counter() - inicio
    tasa = n / seg if seg > 0 else 0
    print(f"  {etiqueta}: {n} registros en {seg:.2f} s ({tasa:,.0f} escrituras/s)")
    return n

def verificar(cliente):
    total = 0
    for ev in EVENTOS:
        for zona in ev["zonas"]:
            clave = clave_zona(ev["id"], zona)
            _, _, bins = cliente.get(key(SET_INVENTARIO, clave))
            if not 0 <= bins[BIN_STOCK] <= bins[BIN_CAPACIDAD]:
                sys.exit(f"Invariante rota en {clave}")
            total += bins[BIN_STOCK]
    return total

def imprimir_resumen_elegante(host, puerto, namespace, eventos, zonas, entradas, usuarios, boletos, semilla):
    """Genera el panel de resumen de inventario con colores ANSI."""
    CYAN = '\033[96m'
    VERDE = '\033[92m'
    AMARILLO = '\033[93m'
    RESET = '\033[0m'
    NEGRITA = '\033[1m'

    print(f"\n{CYAN}{NEGRITA}" + "="*60 + f"{RESET}")
    print(f"{CYAN}{NEGRITA}     🎫 ENTRADAFLASH CR - REPORTE DE INVENTARIO 🎫     {RESET}")
    print(f"{CYAN}{NEGRITA}" + "="*60 + f"{RESET}")
    
    print(f"{NEGRITA}{'Métrica':<32} | {'Valor':>23}{RESET}")
    print("-" * 60)
    
    print(f"{'🎯 Destino':<32} | {f'{host}:{puerto}':>23}")
    print(f"{'📁 Namespace':<32} | {namespace:>23}")
    print(f"{'🎤 Total de Eventos':<32} | {eventos:>23}")
    print(f"{'🏟️  Total de Zonas':<32} | {zonas:>23}")
    print(f"{AMARILLO}{'🎟️  Boletos Generados':<32} | {entradas:>23,}{RESET}")
    print(f"{'👥 Usuarios Simulados':<32} | {usuarios:>23,}")
    print(f"{'🎫 Boletos Individuales Activos':<32} | {'Sí' if boletos else 'No':>23}")
    print(f"{'🌱 Semilla (Random)':<32} | {semilla:>23}")
    print("-" * 60)
    
    print(f"{VERDE}{NEGRITA}✅ Verificación exitosa: {entradas:,} entradas en stock{RESET}")
    print(f"{VERDE}Invariante correcta: Cero pérdida de datos.{RESET}")
    print(f"{CYAN}{NEGRITA}" + "="*60 + f"{RESET}\n")

# ---------------------------------------------------------------------------
# Principal
# ---------------------------------------------------------------------------
def main():
    validar_bins()
    parser = argparse.ArgumentParser(description="Carga sintetica de EntradaFlash CR")
    parser.add_argument("--usuarios", type=int, default=0,
                        help="cantidad de usuarios simulados (el caso pide 50000)")
    parser.add_argument("--boletos", action="store_true",
                        help="crear ademas un registro por boleto individual")
    parser.add_argument("--semilla", type=int, default=42,
                        help="semilla para datos reproducibles (por defecto 42)")
    parser.add_argument("--dry-run", action="store_true",
                        help="mostrar lo que se generaria sin conectarse")
    args = parser.parse_args()

    rng = random.Random(args.semilla)
    entradas = total_entradas()
    zonas = sum(len(ev["zonas"]) for ev in EVENTOS)

    print(f"Iniciando carga sintética de EntradaFlash CR hacia {HOST}:{PORT}...")

    if entradas < MINIMO_ENTRADAS:
        sys.exit(f"El inventario ({entradas}) no alcanza el minimo de {MINIMO_ENTRADAS}")

    if args.dry_run:
        print("\n[dry-run] Ejemplos de registros:")
        for gen in (registros_eventos(), registros_inventario()):
            llave, bins = next(gen)
            print(f"  {llave[1]:10s} {llave[2]:24s} {bins}")
        if args.usuarios:
            llave, bins = next(registros_usuarios(args.usuarios, rng))
            print(f"  {llave[1]:10s} {llave[2]:24s} {bins}")
        if args.boletos:
            llave, bins = next(registros_boletos())
            print(f"  {llave[1]:10s} {llave[2]:24s} {bins}")
        print("\n[dry-run] No se escribio nada.")
        return

    cliente, politica, meta = conectar()
    try:
        print("\nEscribiendo...")
        escribir(cliente, politica, meta, registros_eventos(), "eventos")
        escribir(cliente, politica, meta, registros_inventario(), "inventario")
        if args.usuarios:
            escribir(cliente, politica, meta, registros_usuarios(args.usuarios, rng), "usuarios")
        if args.boletos:
            escribir(cliente, politica, meta, registros_boletos(), "boletos")

        # Verifica el stock y despliega el dashboard
        disponibles = verificar(cliente)
        imprimir_resumen_elegante(
            HOST, PORT, NAMESPACE, 
            len(EVENTOS), zonas, disponibles, 
            args.usuarios, args.boletos, args.semilla
        )
    finally:
        cliente.close()

if __name__ == "__main__":
    main()

# Resultados inmediatos de la ráfaga
    _, _, record_post_compra = client.get(key)
    print("\n================ RESULTADOS INMEDIATOS ================")
    print(f"Reservas exitosas: {resultados['exitos']} | Rechazadas: {resultados['fallos']}")
    print(f"Stock restante tras la ráfaga: {record_post_compra['stock']}")

    # Simulación de expiración de carritos no pagados
    print(f"\n[ESPERA] Simulando falta de pago. Esperando {TTL_RESERVA + 2}s para que expire el TTL...")
    time.sleep(TTL_RESERVA + 2)

    # Limpieza de reservas caídas
    procesar_expiraciones(client, reservas_creadas)

    # Verificación final de consistencia
    _, _, record_final = client.get(key)
    print("\n================ VEREDICTO FINAL ================")
    print(f"Stock Inicial (Mañana):  {stock_inicial}")
    print(f"Stock Final (Tarde):     {record_final['stock']}")
    if stock_inicial == record_final['stock']:
        print("VEREDICTO: Consistencia perfecta. Las reservas cayeron y el stock volvió al 100%.")

    client.close()

if __name__ == "__main__":
    simular_concurrencia()