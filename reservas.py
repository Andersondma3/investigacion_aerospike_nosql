import aerospike
from aerospike import exception as ex
import threading
import time

# Configuración de conexión al contenedor Docker
config = {"hosts": [("aerospike", 3000)]}

TTL_RESERVA = 10  # 5 minutos aun me falta ponerlo, es solo para pruebas que esta en 12 segundos

def inicializar_inventario_prueba(client, key, stock_inicial=10):
    """Inicializa un registro de prueba."""
    record = {
        "evento": "Concierto Test",
        "zona": "VIP",
        "stock": stock_inicial,
        "precio": 50000
    }
    client.put(key, record)
    print(f"[SETUP] Inventario inicializado con stock: {stock_inicial}")

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
            
        except ex.AerospikeError as e:
            print(f"[{id_usuario}] ERROR de Base de datos: {e}")
            return False
            
    print(f"[{id_usuario}] FALLO: Demasiada concurrencia tras {max_reintentos} intentos. Intente de nuevo.")
    return False

def crear_reserva_temporal(client, key_inventario, id_usuario, id_reserva):
    """
    Intenta reservar una entrada mediante CAS.
    Si el CAS tiene éxito, crea la reserva temporal y su registro de control.
    """

    # 1. Descontar inventario utilizando CAS
    exito = reservar_entrada_cas(
        client,
        key_inventario,
        id_usuario
    )

    if not exito:
        print(f"[{id_usuario}] No se pudo crear la reserva.")
        return False

    # 2. Clave de la reserva temporal
    key_reserva = (
        "test",
        "reservas",
        f"reserva:{id_reserva}"
    )

    # 3. Datos de la reserva
    reserva = {
        "usuario": id_usuario,
        "cantidad": 1,
        "estado": "pendiente"
    }

    # 4. Crear reserva con TTL de 5 minutos
    client.put(
        key_reserva,
        reserva,
        meta={"ttl": TTL_RESERVA}
    )

    # 5. Registro de control
    key_control = (
        "test",
        "controles",
        f"control:{id_reserva}"
    )

    control = {
        "reserva_id": f"reserva:{id_reserva}",
        "cantidad": 1,
        "inventario": key_inventario[2],
        "estado": "pendiente"
    }

    client.put(
        key_control,
        control
    )

    print(
        f"[{id_usuario}] Reserva temporal creada. "
        f"ID: reserva:{id_reserva}"
    )

    print(
    f"[{id_usuario}] La reserva tendrá "
    f"una duración de {TTL_RESERVA} segundos."
)

    return key_reserva, key_control

def procesar_expiraciones(client, reservas_creadas):
    """
    Revisa las reservas creadas y libera el inventario
    de aquellas que ya expiraron.
    """

    print("\n========== PROCESANDO EXPIRACIONES ==========")

    for key_reserva, key_control in reservas_creadas:

        # 1. Comprobar si la reserva todavía existe
        try:
            client.get(key_reserva)

            print(
                f"{key_reserva[2]} todavía está activa."
            )

            continue

        except aerospike.exception.RecordNotFound:
            print(
                f"{key_reserva[2]} expiró."
            )

        # 2. Obtener el registro de control
        try:
            _, meta_control, control = client.get(key_control)

        except aerospike.exception.RecordNotFound:
            print("No existe el registro de control.")
            continue

        # 3. Evitar liberar dos veces
        if control["estado"] != "pendiente":
            print(
                f"{key_control[2]} ya fue procesado."
            )
            continue

        # 4. Obtener el inventario
        key_inventario = (
            "test",
            "inventario",
            control["inventario"]
        )

        try:
            _, meta_inventario, inventario = client.get(
                key_inventario
            )

        except aerospike.exception.RecordNotFound:
            print("No existe el inventario.")
            continue

        # 5. Devolver la entrada al inventario
        stock_actual = inventario["stock"]
        cantidad = control["cantidad"]

        inventario["stock"] = stock_actual + cantidad

        try:
            client.put(
                key_inventario,
                inventario,
                policy={
                    "gen": aerospike.POLICY_GEN_EQ
                },
                meta=meta_inventario
            )

            print(
                f"Inventario liberado: "
                f"{stock_actual} → {inventario['stock']}"
            )

        except aerospike.exception.RecordGenerationError:
            print(
                "El inventario cambió mientras "
                "se procesaba la expiración."
            )
            continue

        # 6. Marcar el control como procesado
        control["estado"] = "procesada"

        try:
            client.put(
                key_control,
                control,
                policy={
                    "gen": aerospike.POLICY_GEN_EQ
                },
                meta=meta_control
            )

            print(
                f"{key_control[2]} marcado como procesado."
            )

        except aerospike.exception.RecordGenerationError:
            print(
                "El registro de control cambió "
                "mientras se procesaba."
            )

def simular_concurrencia():
    client = aerospike.client(config).connect()

    key = ("test", "inventario", "evt:test:zona:VIP")

    stock_inicial = 10
    total_compradores = 25

    inicializar_inventario_prueba(
        client,
        key,
        stock_inicial
    )

    resultados = {
        "exitos": 0,
        "fallos": 0
    }
# las reservas exitosas quedaran guardadas en reservas_creadas
    lock = threading.Lock()
    reservas_creadas = []

    def tarea_comprador(user_id, numero_reserva):

        resultado = crear_reserva_temporal(
            client,
            key,
            user_id,
            numero_reserva
        )

        with lock:

            if resultado:
                resultados["exitos"] += 1
                reservas_creadas.append(resultado)
            else:
                resultados["fallos"] += 1

    hilos = []

    print(
        f"\n--- Iniciando ráfaga con "
        f"{total_compradores} compradores concurrentes ---"
    )

    inicio = time.time()

    for i in range(total_compradores):

        user_id = f"Usuario_{i+1:02d}"
        numero_reserva = i + 1

        t = threading.Thread(
            target=tarea_comprador,
            args=(user_id, numero_reserva)
        )

        hilos.append(t)
        t.start()

    for t in hilos:
        t.join()

    fin = time.time()

    print("\n================ RESULTADOS DE RESERVAS ================")

    print(
        f"Tiempo total: "
        f"{(fin - inicio)*1000:.2f} ms"
    )

    print(
        f"Reservas exitosas: "
        f"{resultados['exitos']}"
    )

    print(
        f"Reservas rechazadas: "
        f"{resultados['fallos']}"
    )

    # Revisar inventario antes de la expiración
    _, _, record_final = client.get(key)

    print(
        f"Stock antes de la expiración: "
        f"{record_final['stock']}"
    )

    print(
        "\nEsperando 12 segundos para que "
        "expire el TTL..."
    )

    time.sleep(12)

    # Procesar las reservas que expiraron
    procesar_expiraciones(
        client,
        reservas_creadas
    )

    # Revisar inventario después de las expiraciones
    _, _, record_final = client.get(key)

    print("\n================ RESULTADO FINAL ================")

    print(
        f"Stock después de procesar expiraciones: "
        f"{record_final['stock']}"
    )

    client.close()

if __name__ == "__main__":
    simular_concurrencia()
