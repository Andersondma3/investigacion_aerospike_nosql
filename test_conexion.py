import aerospike
import sys

# Conexión al servicio de Aerospike definido en Docker
config = {
    'hosts': [('aerospike', 3000)]
}

try:
    cliente = aerospike.client(config).connect()
    print(" Conexión exitosa con Aerospike Community Edition.")
except Exception as e:
    print(f" Error al conectar: {e}")
    sys.exit(1)

# Formato de clave: (namespace, set, primary_key)
clave = ('test', 'inventario', 'evt:1:zona:vip')

# Los nombres de los bins no deben superar 14 caracteres
datos = {
    'evento': 'Concierto Inaugural',
    'zona': 'VIP',
    'stock': 500,          # Nombre acortado (< 15 caracteres)
    'precio': 45000.0
}

try:
    # 1. Escribir registro
    cliente.put(clave, datos)
    print(" Registro de prueba guardado correctamente.")

    # 2. Leer registro
    (key, metadata, bins) = cliente.get(clave)
    print("\n Datos recuperados desde Aerospike:")
    print(f"Clave (PK): {key[2]}")
    print(f"Generación: {metadata['gen']}")
    print(f"TTL: {metadata['ttl']}")
    print(f"Contenido: {bins}")

finally:
    cliente.close()