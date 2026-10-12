<h3 align="center">EntradaFlash CR</h3>

XS0131 - Gestión de Bases de Datos y Análisis de Información

Universidad de Costa Rica

**Modelo NoSQL:** Clave-Valor

**Tecnología:** Aerospike Community Edition

[Repositorio](https://github.com/Andersondma3/investigacion_aerospike_nosql)

---

### Integrantes

- Jessica González Olivar
- Samuel Hernández Vargas
- Anderson Martínez Abarca
- Kelsy Montenegro Barboza
- Thomas Vargas Valverde
- Kendall Villegas Espinoza

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
- [Alcance del prototipo](#alcance-del-prototipo)

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

El entorno se ejecuta localmente mediante Docker Compose y utiliza dos servicios principales:

- Un contenedor para **Aerospike**, encargado del almacenamiento y gestión de los registros.
- Un contenedor para **Python**, encargado de ejecutar la lógica y las pruebas de la aplicación.

Esto permite mantener un entorno reproducible sin necesidad de instalar directamente Python o Aerospike en el sistema operativo del equipo.

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

- `evt:{id}` identifica un evento.
- `evt:{id}:zona:{zona}` identifica el inventario de una zona.
- `res:{id}` identifica una reserva.
- `ses:usr:{id}` identifica una sesión temporal.
- `rl:usr:{id}` identifica el registro utilizado para controlar intentos.

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

Separar el inventario por evento y zona permite que solicitudes dirigidas a zonas diferentes trabajen sobre registros independientes y evita generar contención innecesaria sobre una única clave.

Para controlar modificaciones concurrentes sobre un mismo inventario se utiliza **control optimista de concurrencia** mediante la generación del registro de Aerospike.

La generación puede entenderse como una versión del registro. Antes de aceptar una actualización, se comprueba que la versión leída por la aplicación continúe siendo la actual. Si otro proceso modificó el registro previamente, la escritura desactualizada es rechazada y la aplicación puede volver a consultar el estado antes de realizar otro intento.

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
Configura el entorno utilizado para ejecutar Aerospike y la aplicación en Python mediante contenedores independientes.

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
Permite comprobar la persistencia de los datos después de reiniciar o recrear el entorno.

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

Este comando levanta los servicios definidos en `docker-compose.yml`, incluyendo el contenedor de Aerospike y el contenedor utilizado para ejecutar la aplicación en Python.

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

Comprueba que la aplicación en Python pueda comunicarse correctamente con Aerospike antes de ejecutar el resto de las pruebas.

#### Generar y cargar los datos

```bash
docker compose exec app python generar_datos.py
```

Genera los eventos, zonas e inventario inicial que serán utilizados por las demás pruebas.

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

Las reservas temporales utilizan **TTL (Time To Live)** para limitar el tiempo durante el cual permanecen activas. Si una reserva no se completa y expira, la lógica de la aplicación permite devolver el inventario correspondiente.

El control de concurrencia evita que varias solicitudes basadas en una misma versión del inventario actualicen incorrectamente el registro.

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

Estos valores se utilizan con fines demostrativos y pueden modificarse según las necesidades de una implementación real.

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

Las pruebas permiten observar cómo cambia el comportamiento del sistema cuando aumenta la concurrencia.

Las lecturas y escrituras sobre registros independientes pueden aprovechar mejor el aumento de hilos, mientras que múltiples actualizaciones sobre una misma clave generan mayor contención.

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

Esta prueba permite observar el comportamiento del sistema cuando una gran cantidad de solicitudes compite por un inventario limitado y verificar que no se produzca sobreventa.

Una vez agotado el inventario, los intentos adicionales son rechazados correctamente. Estos rechazos representan el comportamiento esperado del sistema y no errores técnicos.

---

### Persistencia y recuperación

Aerospike utiliza un volumen persistente de Docker para almacenar sus datos fuera del ciclo de vida del contenedor.

El volumen **no es otro contenedor**. Es un espacio de almacenamiento administrado por Docker que permanece disponible aunque el contenedor de Aerospike se detenga o sea recreado.

Esto permite recuperar los registros si, por ejemplo:

- El contenedor se detiene debido a un error.
- El contenedor se elimina accidentalmente.
- Es necesario recrearlo después de un cambio de configuración.
- El entorno se reinicia manteniendo el volumen existente.

Primero se crea un registro de prueba:

```bash
docker compose exec app python prueba_recuperacion.py crear
```

Después se detienen los contenedores:

```bash
docker compose down
```

Este comando elimina los contenedores, pero conserva el volumen persistente.

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

Es importante distinguir entre **persistencia** y **alta disponibilidad**.

La persistencia permite que los datos puedan recuperarse después de recrear el contenedor.

Sin embargo, el proyecto utiliza una única instancia de Aerospike. Mientras esa instancia se encuentre fuera de servicio, la aplicación no dispone de otro nodo al cual enviar las solicitudes.

Por esta razón, la persistencia local comprobada en el proyecto no representa alta disponibilidad ni replicación entre nodos.

---

### Alcance del prototipo

La implementación permite demostrar:

- Acceso directo a registros mediante claves conocidas.
- Administración de inventario por evento y zona.
- Reservas temporales.
- Confirmación, cancelación y expiración de reservas.
- Control optimista de concurrencia.
- Prevención de sobreventa.
- Sesiones temporales.
- Control de intentos.
- Persistencia local mediante volúmenes de Docker.
- Pruebas de rendimiento bajo diferentes niveles de concurrencia.
- Pruebas de carga con usuarios e intentos de reserva simulados.

El proyecto fue evaluado utilizando **una única instancia de Aerospike en un entorno local mediante Docker**.

Por lo tanto, el prototipo no pretende demostrar:

- Alta disponibilidad.
- Replicación entre servidores.
- Tolerancia a fallos entre múltiples nodos.
- Rendimiento de un clúster distribuido.
- Rendimiento máximo de Aerospike en ambientes de producción.

Como trabajo futuro, la solución puede extenderse hacia una arquitectura con múltiples nodos, replicación y tolerancia a fallos.
