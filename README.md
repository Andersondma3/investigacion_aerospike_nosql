### EntradaFlash CR

**XS0131 - Gestión de Bases de Datos y Análisis de Información**

**Universidad de Costa Rica**

**Modelo NoSQL:** Clave-Valor

**Tecnología:** Aerospike Community Edition

[Repositorio](https://github.com/Andersondma3/investigacion_aerospike_nosql)

---

### Integrantes

- Jessica González
- Samuel Hernández
- Anderson Martínez
- Kelsy Montenegro
- Thomas Vargas
- Kendall Villegas

---

### Contenido

- [Descripción](#descripción)
- [Herramientas utilizadas](#herramientas-utilizadas)
- [Modelo de datos](#modelo-de-datos)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Requisitos](#requisitos)
- [Guía de uso](#guía-de-uso)
- [Pruebas](#pruebas)
- [Persistencia y recuperación](#persistencia-y-recuperación)

---

### Descripción

EntradaFlash CR es una plataforma ficticia de venta de entradas para conciertos, partidos y otros espectáculos.

El proyecto implementa una solución NoSQL basada en el modelo **Clave-Valor** utilizando **Aerospike Community Edition**. La idea principal es manejar inventario y reservas en escenarios donde muchas personas pueden intentar reservar entradas al mismo tiempo, evitando problemas como inventarios negativos o sobreventa.

La solución trabaja con reservas temporales, confirmaciones, cancelaciones, sesiones, control de intentos, persistencia y pruebas de rendimiento bajo distintos niveles de concurrencia.

El proyecto también cuenta con un archivo `main.py` que permite ejecutar de forma ordenada los principales componentes y pruebas de la solución.

---

### Herramientas utilizadas

- Python 3.11
- Aerospike Community Edition
- Docker
- Docker Compose
- Cliente de Aerospike para Python

---

### Modelo de datos

La solución utiliza claves simples para identificar los diferentes tipos de registros almacenados en Aerospike.

```text
evt:{id}
evt:{id}:zona:{zona}
res:{id}
ses:usr:{id}
rl:usr:{id}
```

Cada tipo de clave tiene un propósito dentro de la solución:

- `evt:{id}` identifica un evento
- `evt:{id}:zona:{zona}` identifica el inventario de una zona
- `res:{id}` identifica una reserva
- `ses:usr:{id}` identifica una sesión temporal
- `rl:usr:{id}` identifica el registro utilizado para controlar intentos

Los registros de inventario por zona manejan información como:

```text
evt_id
zona
capacidad
disponibles
reservados
vendidos
precio
```

Para controlar modificaciones concurrentes sobre un mismo inventario se utiliza **control optimista de concurrencia** mediante la generación del registro de Aerospike.

El conjunto de datos utilizado contiene:

- 5 eventos
- 14 zonas
- 24 200 entradas

Para la prueba general de carga se simulan:

- 50 000 usuarios
- 100 000 intentos de reserva

---

### Estructura del proyecto

```text
investigacion_aerospike_nosql/
│
├── benchmark.py
├── docker-compose.yml
├── generar_datos.py
├── main.py
├── metricas.py
├── prueba_recuperacion.py
├── rate_limit.py
├── reservas.py
├── sesiones.py
├── test_conexion.py
└── README.md
```

#### Archivos principales

**`main.py`**  
Orquesta la ejecución de los principales componentes del proyecto y permite ejecutar el flujo general de pruebas desde un único punto de entrada.

**`docker-compose.yml`**  
Configura el entorno utilizado para ejecutar Aerospike y la aplicación en Python.

**`generar_datos.py`**  
Genera y carga los eventos, zonas e inventario utilizados durante las pruebas.

**`reservas.py`**  
Contiene la lógica de reservas temporales, confirmación, cancelación, expiración y operaciones concurrentes sobre el inventario.

**`sesiones.py`**  
Maneja el almacenamiento y recuperación de sesiones temporales.

**`rate_limit.py`**  
Contiene la lógica y la prueba utilizada para controlar la cantidad de intentos realizados por un usuario.

**`metricas.py`**  
Ejecuta las pruebas de rendimiento para operaciones de lectura, escritura y actualización atómica.

**`benchmark.py`**  
Ejecuta la prueba general de carga con múltiples solicitudes concurrentes.

**`prueba_recuperacion.py`**  
Permite comprobar la persistencia de los datos después de reiniciar el entorno.

**`test_conexion.py`**  
Verifica la conexión entre Aerospike y Python.

---

### Requisitos

Para ejecutar el proyecto localmente se necesita:

- Git
- Docker
- Docker Compose

Python y Aerospike **no necesitan instalarse directamente en el equipo**, ya que ambos se ejecutan dentro del entorno configurado con Docker.

---

### Guía de uso

Si es la primera vez que se ejecuta el proyecto, se recomienda seguir los pasos en este orden.

#### 1. Clonar el repositorio

```bash
git clone https://github.com/Andersondma3/investigacion_aerospike_nosql.git
cd investigacion_aerospike_nosql
```

#### 2. Levantar el entorno

```bash
docker compose up -d
```

#### 3. Comprobar los servicios

```bash
docker compose ps
```

#### 4. Ejecutar el flujo completo

El archivo `main.py` permite ejecutar de forma ordenada los principales componentes y pruebas del proyecto.

```bash
docker compose exec app python main.py
```

Durante esta ejecución se realizan la validación de conexión, la generación de datos, las pruebas de reservas, sesiones, control de intentos, carga y métricas de rendimiento.

Las pruebas también pueden ejecutarse individualmente utilizando los comandos de la siguiente sección.

---

### Pruebas

#### Verificar conexión con Aerospike

```bash
docker compose exec app python test_conexion.py
```

#### Generar y cargar los datos

```bash
docker compose exec app python generar_datos.py
```

#### Reservas y concurrencia

```bash
docker compose exec app python reservas.py
```

Esta prueba permite comprobar:

- Creación de reservas temporales
- Confirmación de reservas
- Cancelación de reservas
- Expiración de reservas
- Devolución de inventario
- Operaciones concurrentes sobre una misma zona
- Prevención de sobreventa

#### Sesiones

```bash
docker compose exec app python sesiones.py
```

Permite comprobar la creación y recuperación de una sesión temporal asociada a un usuario.

#### Control de intentos

```bash
docker compose exec app python rate_limit.py
```

La prueba utiliza:

- Máximo de 3 intentos
- Ventana de 10 segundos

Los intentos adicionales son rechazados temporalmente hasta que finaliza la ventana.

#### Rendimiento

```bash
docker compose exec app python metricas.py
```

Se evalúan:

- Lectura
- Escritura
- Actualización atómica

Cada escenario ejecuta **5 000 operaciones**.

Los niveles de concurrencia utilizados son:

```text
1 hilo
8 hilos
16 hilos
32 hilos
64 hilos
```

Las métricas obtenidas incluyen:

- Operaciones por segundo
- Latencia promedio
- p50
- p95
- p99
- Operaciones correctas
- Operaciones fallidas

#### Prueba general de carga

```bash
docker compose exec app python benchmark.py
```

La prueba utiliza:

```text
Usuarios simulados:        50 000
Intentos de reserva:      100 000
Hilos concurrentes:            32
```

El benchmark toma el inventario disponible al iniciar la prueba y comprueba que las reservas realizadas coincidan con el inventario final.

Esta prueba permite comprobar el comportamiento del sistema cuando una gran cantidad de solicitudes compite por un inventario limitado y verificar que no se produzca sobreventa.

---

### Persistencia y recuperación

Primero se crea un registro de prueba:

```bash
docker compose exec app python prueba_recuperacion.py crear
```

Después se detienen los contenedores:

```bash
docker compose down
```

Se levanta nuevamente el entorno:

```bash
docker compose up -d
```

Finalmente se verifica que el registro continúe disponible:

```bash
docker compose exec app python prueba_recuperacion.py verificar
```

Para detener el entorno conservando los datos:

```bash
docker compose down
```

Para detener el entorno y eliminar también el volumen persistente:

```bash
docker compose down -v
```

El segundo comando elimina los datos almacenados en el volumen, por lo que debe utilizarse solamente cuando se quiera reiniciar completamente el entorno.

---

**EntradaFlash CR**