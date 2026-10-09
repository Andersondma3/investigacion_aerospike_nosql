import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

PASOS = [
    (
        "test_conexion.py",
        "validacion de conexion con Aerospike"
    ),
    (
        "generar_datos.py",
        "Generacion y carga de datos"
    ),
    (
        "reservas.py",
        "Reservas, concurrencia y expiracion TTL"
    ),
    (
        "sesiones.py",
        "Gestion de sesiones temporales"
    ), 
    (
        "rate_limit.py",
        "Control de solicitudes por usuario"
    ),
    (
        "benchmark.py",
        "Pruebas de rendimiento y concurrencia"
    ),
    (
        "metricas.py",
        "Calculo y analisis de metrica"
    )
]

def ejecutar_scrip(nombre_archivo, descripcion):
    ruta = BASE_DIR / nombre_archivo

    print("\n" + "-" * 70)
    print(f"INICIADO: {descripcion}")
    print(f"ARCHIVO: {nombre_archivo}")
    print("-" * 70)

    if not ruta.exists(): 
        print(f"ERROR: No se encintro {nombre_archivo}")
        return False

    resultado = subprocess.run(
        [sys.executable, str(ruta)],
        cwd=BASE_DIR
    )

    if resultado.returncode !=0:
        print("\n" + "-" * 70)
        print(f"EROR en: {descripcion}")
        print(f"Codigo de salida: {resultado.returncode}")
        print("-" * 70)
        return False

    print(f"\nOK: {descripcion}")
    return True

def main():
    print("\n")
    print("=" * 70)
    print("ENTRADAFLASH CR")
    print("ORQUESTACION GENERAL - AEROSPIKE")
    print("=" * 70)

    completado = 0 

    for archivo, descripcion in PASOS:
        correcto = ejecutar_scrip(
            archivo, 
            descripcion
        )

        if not correcto:
            print("\n" + "-" * 70)
            print("EJECUCION INTERRUMPIDA")
            print(f"Ultimo paso: {descripcion}")
            print("=" * 70)

            return 1
        
        completado += 1

    print("\n" + "-" * 70)
    print("EJECUCION COMPLETA FINALIZADA")
    print(f"Pasos completados: {completado} / {len(PASOS)}")
    print("=" * 70)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())