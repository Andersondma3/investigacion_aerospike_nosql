import aerospike
from aerospike import exception as ex
import threading
import time
import os

# ==========================================
# CONFIGURACIÓN (Anderson - Entorno Docker)
# ==========================================
_EN_DOCKER = os.path.exists("/.dockerenv")
HOST = os.getenv("AEROSPIKE_HOST", "aerospike" if _EN_DOCKER else "127.0.0.1")
PORT = int(os.getenv("AEROSPIKE_PORT", "3000"))
NAMESPACE = "test"
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
            key_tuple, meta, bins = client.get(key)
            stock_actual = bins.get('stock', 0)
            
            if stock_actual < 1:
                print(f"[{id_usuario}] RECHAZADO: Boletos agotados.")
                return False
            
            bins['stock'] = stock_actual - 1
            politica = {'gen': aerospike.POLICY_GEN_EQ}
            client.put(key, bins, policy=politica, meta=meta)
            
            return True
            
        except ex.RecordGenerationError:
            intentos += 1
            time.sleep(0.01) 
        except ex.RecordNotFound:
            print(f"[{id_usuario}] ERROR: El evento no existe. ¿Ejecutaste generar_datos.py primero?")
            return False
        except ex.AerospikeError as e:
            print(f"[{id_usuario}] ERROR de Base de datos: {e}")
            return False
            
    print(f"[{id_usuario}] FALLO: Demasiada concurrencia tras {max_reintentos} intentos.")
    return False

# ==========================================
# MÓDULO 2: GESTIÓN DE EXPIRACIÓN TTL (Samuel)
# ==========================================
def crear_reserva_temporal(client, key_inventario, id_usuario, id_reserva):
    exito = reservar_entrada_cas(client, key_inventario, id_usuario)
    if not exito:
        return False

    key_reserva = (NAMESPACE, "reservas", f"reserva:{id_reserva}")
    reserva = {"usuario": id_usuario, "cantidad": 1, "estado": "pendiente"}
    client.put(key_reserva, reserva, meta={"ttl": TTL_RESERVA})

    key_control = (NAMESPACE, "controles", f"control:{id_reserva}")
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
        try:
            client.get(key_reserva)
            print(f"-> {key_reserva[2]} todavía activa (Usuario pagó o está en tiempo).")
            continue
        except ex.RecordNotFound:
            print(f"-> {key_reserva[2]} EXPIRÓ. Iniciando devolución de stock...")

        try:
            _, meta_control, control = client.get(key_control)
        except ex.RecordNotFound:
            continue
            
        if control["estado"] != "pendiente":
            continue

        key_inventario = (NAMESPACE, "inventario", control["inventario"])
        cantidad_a_devolver = control["cantidad"]

        intentos = 0
        devuelto = False
        while intentos < 10 and not devuelto:
            try:
                _, meta_inventario, inventario = client.get(key_inventario)
                inventario["stock"] += cantidad_a_devolver
                client.put(key_inventario, inventario, policy={"gen": aerospike.POLICY_GEN_EQ}, meta=meta_inventario)
                print(f"   [+] Inventario recuperado: ahora hay {inventario['stock']} boletos.")
                devuelto = True
            except ex.RecordGenerationError:
                intentos += 1
                time.sleep(0.01)

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

    key = (NAMESPACE, "inventario", "evt:1:zona:VIP")
    
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

    print(f"\n--- Ráfaga de {total_compradores} compradores concurrentes ---")
    inicio = time.time()
    
    for i in range(total_compradores):
        t = threading.Thread(target=tarea_comprador, args=(f"Usuario_{i+1:02d}", i+1))
        hilos.append(t)
        t.start()

    for t in hilos:
        t.join()
        
    fin = time.time()

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