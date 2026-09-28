import aerospike
from aerospike import exception as ex
import threading
import time
import os

# Configuración de conexión adaptada para funcionar igual que el script de Kelsy
_EN_DOCKER = os.path.exists("/.dockerenv")
HOST = os.getenv("AEROSPIKE_HOST", "aerospike" if _EN_DOCKER else "127.0.0.1")
PORT = int(os.getenv("AEROSPIKE_PORT", "3000"))
config = {"hosts": [(HOST, PORT)]}

def reservar_entrada_cas(client, key, id_usuario):
    """
    Intenta reservar 1 unidad usando Optimistic Locking (CAS).
    Si hay colisión, reintenta automáticamente.
    """
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
            else:
                resultados["fallos"] += 1

    hilos = []
    print(f"\n--- Iniciando ráfaga con {total_compradores} compradores concurrentes ---")
    inicio = time.time()
    
    for i in range(total_compradores):
        t = threading.Thread(target=tarea_comprador, args=(f"Usuario_{i+1:02d}",))
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