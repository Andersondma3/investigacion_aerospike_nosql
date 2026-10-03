import aerospike
from aerospike import exception as ex
import os
import time


# Configuración de Aerospike

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


# Permitimos 3 intentos cada 10 segundos

LIMITE_INTENTOS = 3
VENTANA_SEGUNDOS = 10


# Revisamos si el usuario puede hacer otro intento

def permitir_solicitud(
    client,
    id_usuario
):

    key = (
        NAMESPACE,
        "rate_limit",
        f"usuario:{id_usuario}"
    )

    reintentos = 0

    while reintentos < 10:

        try:

            _, meta, datos = client.get(
                key
            )

            intentos_actuales = datos.get(
                "intentos",
                0
            )

            # Si llegó al límite lo bloqueamos

            if intentos_actuales >= LIMITE_INTENTOS:

                print(
                    f"[{id_usuario}] Solicitud bloqueada. "
                    f"Alcanzó el límite de {LIMITE_INTENTOS} intentos."
                )

                return False


            datos["intentos"] = intentos_actuales + 1

            try:

                client.put(
                    key,
                    datos,
                    policy={
                        "gen": aerospike.POLICY_GEN_EQ
                    },
                    meta={
                        "gen": meta["gen"],
                        "ttl": VENTANA_SEGUNDOS
                    }
                )

                print(
                    f"[{id_usuario}] Solicitud permitida. "
                    f"Intento {intentos_actuales + 1} "
                    f"de {LIMITE_INTENTOS}."
                )

                return True

            except ex.RecordGenerationError:

                reintentos += 1
                time.sleep(0.01)


        except ex.RecordNotFound:

            client.put(
                key,
                {
                    "intentos": 1
                },
                meta={
                    "ttl": VENTANA_SEGUNDOS
                }
            )

            print(
                f"[{id_usuario}] Solicitud permitida. "
                f"Intento 1 de {LIMITE_INTENTOS}."
            )

            return True


    print(
        f"[{id_usuario}] No se pudo procesar "
        f"la solicitud."
    )

    return False


# Probamos el límite

def probar_rate_limit():

    client = aerospike.client(
        config
    ).connect()

    usuario = "Usuario_01"

    print(
        f"Probando límite de solicitudes para {usuario}..."
    )


    # Hacemos 5 intentos seguidos

    for numero in range(
        1,
        6
    ):

        print(
            f"\nIntento {numero}:"
        )

        permitir_solicitud(
            client,
            usuario
        )

    print(
        f"\nEsperando {VENTANA_SEGUNDOS + 2} segundos..."
    )

    time.sleep(
        VENTANA_SEGUNDOS + 2
    )


    print(
        "\nIntento después de esperar:"
    )

    permitir_solicitud(
        client,
        usuario
    )


    client.close()


# Ejecutamos

if __name__ == "__main__":
    probar_rate_limit()