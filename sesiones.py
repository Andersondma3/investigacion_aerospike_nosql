import aerospike
from aerospike import exception as ex
import os

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

# Definimos un tiempo limitado para la sesión 

TTL_SESION = 60

# Guardamos el carrito de un usuario

def guardar_sesion(
    client,
    id_usuario,
    evento,
    zona,
    cantidad
):

    key = (
        NAMESPACE,
        "sesiones",
        f"sesion:{id_usuario}"
    )

    carrito = {
        "usuario": id_usuario,
        "evento": evento,
        "zona": zona,
        "cantidad": cantidad
    }

    client.put(
        key,
        carrito,
        meta={
            "ttl": TTL_SESION
        }
    )

    print(
        f"Sesión guardada para {id_usuario}."
    )

    return key

# Recuperamos el carrito por la clave del usuario

def recuperar_sesion(
    client,
    id_usuario
):

    key = (
        NAMESPACE,
        "sesiones",
        f"sesion:{id_usuario}"
    )

    try:

        _, _, carrito = client.get(
            key
        )

        print(
            f"Sesión recuperada para {id_usuario}:"
        )

        print(
            f"Evento:   {carrito['evento']}"
        )

        print(
            f"Zona:     {carrito['zona']}"
        )

        print(
            f"Cantidad: {carrito['cantidad']}"
        )

        return carrito

    except ex.RecordNotFound:

        print(
            f"No existe una sesión activa "
            f"para {id_usuario}."
        )

        return None

# Eliminamos la sesión si el usuario termina

def eliminar_sesion(
    client,
    id_usuario
):

    key = (
        NAMESPACE,
        "sesiones",
        f"sesion:{id_usuario}"
    )

    try:

        client.remove(
            key
        )

        print(
            f"Sesión eliminada para {id_usuario}."
        )

        return True

    except ex.RecordNotFound:

        return False

# Probamos que la sesión se pueda recuperar

def probar_sesion():

    client = aerospike.client(
        config
    ).connect()

    print(
        "Guardando carrito temporal..."
    )

    guardar_sesion(
        client,
        "Usuario_01",
        "Concierto Inaugural",
        "VIP",
        2
    )

    client.close()

    client = aerospike.client(
        config
    ).connect()

    print(
        "\nEl usuario vuelve al sistema..."
    )

    carrito = recuperar_sesion(
        client,
        "Usuario_01"
    )

    if carrito:

        print(
            "\nResultado: el carrito se recuperó correctamente."
        )

    else:

        print(
            "\nResultado: no se pudo recuperar el carrito."
        )

    client.close()

# Ejecutamos

if __name__ == "__main__":
    probar_sesion()