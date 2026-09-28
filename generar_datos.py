"""
Carga sintetica y reproducible de EntradaFlash CR en Aerospike.
"""

import argparse
import os
import random
import sys
import time

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
# IMPORTANTE: el bin de inventario debe llamarse exactamente "stock",
# porque el algoritmo CAS de reservas.py descuenta sobre ese nombre.
BIN_EVT_ID = "evt_id"
BIN_EVENTO = "evento"
BIN_ZONA = "zona"
BIN_STOCK = "stock"
BIN_CAPACIDAD = "capacidad"
BIN_PRECIO = "precio"

# ---Registro de evento----
BIN_NOMBRE = "nombre"
BIN_FECHA = "fecha"
BIN_LUGAR = "lugar"
BIN_ZONAS = "zonas"

# ---Registro de usuario----
BIN_USR_ID = "usr_id"
BIN_EMAIL = "email"

# ---Registro de boleto individual (opcional)---
BIN_NUMERO = "numero"
BIN_ESTADO = "estado"

MAX_LARGO_BIN = 14


def validar_bins():
    """Verificación: todos los nombres de bins cumplen la regla del equipo."""
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


def clave_sesion(usr_id):
    return f"ses:usr:{usr_id:05d}"


def clave_ratelimit(usr_id):
    return f"rl:usr:{usr_id:05d}"


def key(set_name, clave):
    """Tupla de clave de Aerospike: (namespace, set, clave)."""
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
# Construccion de registros (sin conexion)
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
# Escritura en Aerospike
# -------------------------------------
def conectar():
    import aerospike  

    config = {"hosts": [(HOST, PORT)]}
    try:
        cliente = aerospike.client(config).connect()
    except Exception as exc:  # noqa: BLE001
        sys.exit(f"No se pudo conectar a Aerospike en {HOST}:{PORT} -> {exc}")
    politica = {
        "key": aerospike.POLICY_KEY_SEND,             # guarda el texto de la clave
    }
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
    """Relee el inventario y comprueba que 0 <= stock <= capacidad en cada zona."""
    total = 0
    for ev in EVENTOS:
        for zona in ev["zonas"]:
            clave = clave_zona(ev["id"], zona)
            _, _, bins = cliente.get(key(SET_INVENTARIO, clave))
            if not 0 <= bins[BIN_STOCK] <= bins[BIN_CAPACIDAD]:
                sys.exit(f"Invariante rota en {clave}")
            total += bins[BIN_STOCK]
    return total


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

    print("EntradaFlash CR - generacion de datos")
    print(f"  Destino: {HOST}:{PORT}  namespace='{NAMESPACE}'")
    print(f"  Eventos: {len(EVENTOS)}  Zonas: {zonas}  Entradas: {entradas:,}")
    print(f"  Usuarios: {args.usuarios:,}  Boletos individuales: {'si' if args.boletos else 'no'}")
    print(f"  Semilla: {args.semilla}")

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

        disponibles = verificar(cliente)
        print(f"\nVerificacion: {disponibles:,} entradas en stock; invariante correcta.")
    finally:
        cliente.close()


if __name__ == "__main__":
    main()
