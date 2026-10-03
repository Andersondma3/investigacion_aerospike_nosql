import aerospike
from aerospike import exception as ex
import threading
import time
import os


# Configuración de Aerospike (Anderson)

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


# Tiempo de vida de las reservas (Samuel)

TTL_RESERVA = 10


# Reservas con CAS (Anderson)

def reservar_entrada_cas(client, key, id_usuario):

    max_reintentos = 10
    intentos = 0

    while intentos < max_reintentos:

        try:

            _, meta, bins = client.get(key)

            stock_actual = bins.get(
                "stock",
                0
            )

            if stock_actual < 1:

                print(
                    f"[{id_usuario}] RECHAZADO: "
                    f"Boletos agotados."
                )

                return False

            bins["stock"] = stock_actual - 1

            politica = {
                "gen": aerospike.POLICY_GEN_EQ
            }

            meta_cas = {
                "gen": meta["gen"]
            }

            client.put(
                key,
                bins,
                policy=politica,
                meta=meta_cas
            )

            return True

        except ex.RecordGenerationError:

            intentos += 1
            time.sleep(0.01)

        except ex.RecordNotFound:

            print(
                f"[{id_usuario}] ERROR: "
                f"El evento no existe."
            )

            return False

        except ex.AerospikeError as e:

            print(
                f"[{id_usuario}] ERROR de base de datos: "
                f"{e}"
            )

            return False

    print(
        f"[{id_usuario}] FALLO: "
        f"Demasiada concurrencia."
    )

    return False


# Creamos una reserva temporal (Samuel)

def crear_reserva_temporal(
    client,
    key_inventario,
    id_usuario,
    id_reserva
):

    exito = reservar_entrada_cas(
        client,
        key_inventario,
        id_usuario
    )

    if not exito:
        return False

    key_reserva = (
        NAMESPACE,
        "reservas",
        f"reserva:{id_reserva}"
    )

    reserva = {
        "usuario": id_usuario,
        "cantidad": 1,
        "estado": "pendiente"
    }

    client.put(
        key_reserva,
        reserva,
        meta={
            "ttl": TTL_RESERVA
        }
    )

    key_control = (
        NAMESPACE,
        "controles",
        f"control:{id_reserva}"
    )

    control = {
        "reserva_id": f"reserva:{id_reserva}",
        "usuario": id_usuario,
        "cantidad": 1,
        "inventario": key_inventario[2],
        "estado": "pendiente"
    }

    client.put(
        key_control,
        control
    )

    print(
        f"[{id_usuario}] ÉXITO: "
        f"Reserva temporal creada. "
        f"Expira en {TTL_RESERVA}s."
    )

    return key_reserva, key_control


# Cambiamos el estado de una reserva (Kendall)

def cambiar_estado_control(
    client,
    key_control,
    estado_actual,
    nuevo_estado
):

    intentos = 0

    while intentos < 10:

        try:

            _, meta, control = client.get(
                key_control
            )

            if control.get("estado") != estado_actual:
                return False, control

            control["estado"] = nuevo_estado

            try:

                client.put(
                    key_control,
                    control,
                    policy={
                        "gen": aerospike.POLICY_GEN_EQ
                    },
                    meta={
                        "gen": meta["gen"]
                    }
                )

                return True, control

            except ex.RecordGenerationError:

                intentos += 1
                time.sleep(0.01)

        except ex.RecordNotFound:

            return False, None

    return False, None


# Devolvemos el boleto al inventario (Kendall)

def devolver_stock(client, control):

    key_inventario = (
        NAMESPACE,
        "inventario",
        control["inventario"]
    )

    cantidad = control.get(
        "cantidad",
        1
    )

    intentos = 0

    while intentos < 10:

        try:

            _, meta, inventario = client.get(
                key_inventario
            )

            inventario["stock"] += cantidad

            try:

                client.put(
                    key_inventario,
                    inventario,
                    policy={
                        "gen": aerospike.POLICY_GEN_EQ
                    },
                    meta={
                        "gen": meta["gen"]
                    }
                )

                return True

            except ex.RecordGenerationError:

                intentos += 1
                time.sleep(0.01)

        except ex.RecordNotFound:

            return False

    return False


# Confirmamos una reserva (Kendall)

def confirmar_reserva(client, id_reserva):

    key_reserva = (
        NAMESPACE,
        "reservas",
        f"reserva:{id_reserva}"
    )

    key_control = (
        NAMESPACE,
        "controles",
        f"control:{id_reserva}"
    )

    # Revisamos que la reserva todavía exista

    try:

        client.get(
            key_reserva
        )

    except ex.RecordNotFound:

        print(
            f"[Reserva {id_reserva}] "
            f"No se puede confirmar porque ya expiró."
        )

        return False

    # Solo confirmamos si todavía está pendiente

    cambiado, _ = cambiar_estado_control(
        client,
        key_control,
        "pendiente",
        "confirmada"
    )

    if not cambiado:

        print(
            f"[Reserva {id_reserva}] "
            f"No se pudo confirmar porque "
            f"ya fue procesada."
        )

        return False

    # Quitamos la reserva temporal porque ya quedó confirmada

    try:

        client.remove(
            key_reserva
        )

    except ex.RecordNotFound:

        pass

    print(
        f"[Reserva {id_reserva}] "
        f"CONFIRMADA correctamente."
    )

    return True


# Cancelamos una reserva (Kendall)

def cancelar_reserva(client, id_reserva):

    key_reserva = (
        NAMESPACE,
        "reservas",
        f"reserva:{id_reserva}"
    )

    key_control = (
        NAMESPACE,
        "controles",
        f"control:{id_reserva}"
    )

    # Cambiamos el estado primero para no devolver el boleto dos veces

    cambiado, control = cambiar_estado_control(
        client,
        key_control,
        "pendiente",
        "cancelando"
    )

    if not cambiado:

        print(
            f"[Reserva {id_reserva}] "
            f"No se pudo cancelar porque "
            f"ya fue procesada."
        )

        return False

    # Devolvemos el boleto

    if not devolver_stock(
        client,
        control
    ):

        cambiar_estado_control(
            client,
            key_control,
            "cancelando",
            "pendiente"
        )

        print(
            f"[Reserva {id_reserva}] "
            f"Error al devolver el stock."
        )

        return False

    cambiar_estado_control(
        client,
        key_control,
        "cancelando",
        "cancelada"
    )

    try:

        client.remove(
            key_reserva
        )

    except ex.RecordNotFound:

        pass

    print(
        f"[Reserva {id_reserva}] "
        f"CANCELADA. El boleto volvió al inventario."
    )

    return True


# Procesamos las reservas expiradas (Samuel + Kendall)

def procesar_expiraciones(
    client,
    reservas_creadas
):

    print(
        "\nProcesando reservas expiradas..."
    )

    for key_reserva, key_control in reservas_creadas:

        try:

            client.get(
                key_reserva
            )

            print(
                f"-> {key_reserva[2]} todavía activa."
            )

            continue

        except ex.RecordNotFound:

            print(
                f"-> {key_reserva[2]} expiró."
            )

        # Solo devolvemos el boleto si todavía estaba pendiente

        cambiado, control = cambiar_estado_control(
            client,
            key_control,
            "pendiente",
            "expirando"
        )

        if not cambiado:
            continue

        if devolver_stock(
            client,
            control
        ):

            cambiar_estado_control(
                client,
                key_control,
                "expirando",
                "procesada_y_devuelta"
            )

            print(
                "   Stock devuelto al inventario."
            )


# Simulación de concurrencia

def simular_concurrencia():

    try:

        client = aerospike.client(
            config
        ).connect()

    except Exception as e:

        print(
            f"Error de conexión: {e}"
        )

        return

    key = (
        NAMESPACE,
        "inventario",
        "evt:1:zona:VIP"
    )

    try:

        _, _, record_inicial = client.get(
            key
        )

        stock_inicial = record_inicial.get(
            "stock",
            0
        )

        print(
            f"Conectado al evento. "
            f"Stock inicial: {stock_inicial}"
        )

    except ex.RecordNotFound:

        print(
            "ERROR: Ejecuta generar_datos.py primero."
        )

        client.close()

        return

    total_compradores = 25

    resultados = {
        "exitos": 0,
        "fallos": 0
    }

    lock = threading.Lock()
    reservas_creadas = []

    def tarea_comprador(
        user_id,
        numero_reserva
    ):

        resultado = crear_reserva_temporal(
            client,
            key,
            user_id,
            numero_reserva
        )

        with lock:

            if resultado:

                resultados["exitos"] += 1

                reservas_creadas.append(
                    resultado
                )

            else:

                resultados["fallos"] += 1

    hilos = []

    print(
        f"\nSimulando {total_compradores} "
        f"compradores al mismo tiempo..."
    )

    for i in range(
        total_compradores
    ):

        t = threading.Thread(
            target=tarea_comprador,
            args=(
                f"Usuario_{i + 1:02d}",
                i + 1
            )
        )

        hilos.append(
            t
        )

        t.start()

    for t in hilos:

        t.join()

    _, _, record_post_compra = client.get(
        key
    )

    print(
        "\nResultados de las reservas:"
    )

    print(
        f"Reservas creadas: "
        f"{resultados['exitos']}"
    )

    print(
        f"Reservas rechazadas: "
        f"{resultados['fallos']}"
    )

    print(
        f"Stock después de las reservas: "
        f"{record_post_compra['stock']}"
    )


    # Probamos confirmar y cancelar (Kendall)

    print(
        "\nPrueba de confirmación y cancelación:"
    )

    confirmada = confirmar_reserva(
        client,
        1
    )

    cancelada = cancelar_reserva(
        client,
        2
    )


    # Dejamos que las otras reservas expiren

    print(
        f"\nEsperando {TTL_RESERVA + 2}s "
        f"para que expiren las reservas pendientes..."
    )

    time.sleep(
        TTL_RESERVA + 2
    )

    procesar_expiraciones(
        client,
        reservas_creadas
    )


    # Revisamos cómo quedó el inventario

    _, _, record_final = client.get(
        key
    )

    reservas_confirmadas = (
        1 if confirmada else 0
    )

    stock_esperado = (
        stock_inicial
        - reservas_confirmadas
    )

    print(
        "\nResultado final:"
    )

    print(
        f"Stock inicial:       "
        f"{stock_inicial}"
    )

    print(
        f"Reserva confirmada:  "
        f"{'Sí' if confirmada else 'No'}"
    )

    print(
        f"Reserva cancelada:   "
        f"{'Sí' if cancelada else 'No'}"
    )

    print(
        f"Stock esperado:      "
        f"{stock_esperado}"
    )

    print(
        f"Stock final:         "
        f"{record_final['stock']}"
    )

    if record_final["stock"] == stock_esperado:

        print(
            "Resultado: inventario correcto."
        )

    else:

        print(
            "Resultado: hay una inconsistencia "
            "en el inventario."
        )

    client.close()


# Ejecutamos

if __name__ == "__main__":
    simular_concurrencia()