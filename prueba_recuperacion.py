import sys
import time
import aerospike

CONFIG = {
    "hosts": [("aerospike", 3000)]
}

NAMESPACE = "test"
SET = "recuperacion"
KEY = "prueba_persistencia_001"


def conectar():
    for intento in range(10):
        try:
            return aerospike.client(CONFIG).connect()
        except Exception:
            if intento == 9:
                raise
            time.sleep(1)


def crear():
    client = conectar()
    key = (NAMESPACE, SET, KEY)

    datos = {
        "mensaje": "Prueba de recuperacion de EntradaFlash CR",
        "estado": "guardado"
    }

    client.put(key, datos)

    _, _, registro = client.get(key)

    print("Registro creado correctamente.")
    print("Clave:", KEY)
    print("Contenido:", registro)

    client.close()


def verificar():
    client = conectar()
    key = (NAMESPACE, SET, KEY)

    try:
        _, _, registro = client.get(key)

        print("Registro encontrado.")
        print("Los datos siguen disponibles.")
        print("Contenido:", registro)

    except aerospike.exception.RecordNotFound:
        print("Registro no encontrado.")
        print("Los datos no se conservaron.")

    finally:
        client.close()


if len(sys.argv) != 2:
    print("Uso:")
    print("python prueba_recuperacion.py crear")
    print("python prueba_recuperacion.py verificar")
    sys.exit(1)


accion = sys.argv[1].lower()

if accion == "crear":
    crear()
elif accion == "verificar":
    verificar()
else:
    print("Accion no valida.")