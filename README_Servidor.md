# Servidor externo

## ¿Por qué es útil utilizar un servidor externo?

Aunque podríamos ejecutar los programas y acceder a la base de datos desde cada ordenador personal, hay varias razones por las que utilizar un **servidor externo** es más efectivo:

1. **Disponibilidad 24/7:** Un servidor puede estar encendido todo el tiempo, ejecutando aplicaciones o procesos sin depender de nuestro ordenador.
2. **Acceso remoto:** Puedes conectarte desde cualquier lugar y compartir recursos con otros usuarios.
3. **Mayor potencia:** Los servidores suelen tener a veces más memoria, CPU o almacenamiento que un ordenador personal.
4. **Seguridad y backups:** Los datos se pueden centralizar, hacer copias de seguridad automáticas y controlar permisos.
5. **Escalabilidad:** Si tu proyecto crece (por ejemplo, muchas consultas a la base de datos), un servidor puede ampliarse para soportarlo.

## ¿Cuál es la estructura general de un servidor?

Un servidor típico que aloja aplicaciones, Python y bases de datos suele tener varias capas:

```
[Usuario/Cliente] <--internet--> [Servidor]
                                   ├─ Sistema operativo (Linux suele ser el más común)
                                   ├─ Servidor web (Nginx, Apache)
                                   ├─ Aplicaciones (Python, Node.js, etc.)
                                   ├─ Base de datos (MySQL, PostgreSQL)
                                   └─ Servicios de seguridad y gestión (SSH, firewalls)

```

* **Sistema operativo:** Controla el hardware y permite ejecutar programas.
* **Servidor web (Nginx, Apache):** Atiende solicitudes HTTP/HTTPS y dirige el tráfico a tu aplicación.
* **Aplicaciones:** Por ejemplo, programas en Python que hacen cálculos, procesan datos o sirven páginas web.
* **Base de datos:** Almacena información estructurada accesible mediante SQL.
* **Servicios de acceso y seguridad:** SSH, firewalls, VPN, etc.

```
[Tu ordenador / Cliente]
   ├── SSH (Puerto 22) ──────────────▶ [Servidor] 
   │                                  ┌───────────────────────────────┐
   │                                  │ Sistema Operativo (Linux)     │
   │                                  ├───────────────────────────────┤
   │                                  │ Seguridad / Firewall           │
   │                                  ├───────────────────────────────┤
   │                                  │ Nginx (Servidor Web)           │◀── HTTP/HTTPS (Puertos 80/443)
   │                                  │  ├── App Web Python 1           │
   │                                  │  ├── App Web Python 2           │
   │                                  │  └── App Node.js                │
   │                                  ├───────────────────────────────┤
   │                                  │ Base de Datos                  │
   │                                  │  ├── MySQL (Puerto 3306)       │
   │                                  │  └── PostgreSQL (Puerto 5432) │
   │                                  └───────────────────────────────┘
   │
[Navegador Web]
   └── HTTP/HTTPS (Puertos 80/443) ─▶ Nginx ──▶ Redirige a App Web correspondiente

```

## Los Puertos (puertas del servidor) como vías de entrada desde nuestro ordenador al servidor

Los servidores usan **puertos** para diferenciar servicios:

* **22:** SSH (acceso remoto seguro)
  * Podemos acceder desde **Visual Studio Code** o desde **PowerShell**
  * **Qué es:** Es un protocolo seguro para acceder a la línea de comandos de un servidor remoto
  * **Por qué se usa:** Permite ejecutar programas, transferir archivos y administrar el servidor de forma segura
  * **Cómo se usa:**`ssh usuario@direccion_del_servidor`
* **80:** HTTP (páginas web sin cifrado) --> acceso vía **NGINX**
  * Podemos acceder desde un **navegador web** (Chrome, Edge, etc.) utilizando la IP del servidor (10.32.7.60)
* **443:** HTTPS (páginas web cifradas) --> acceso vía **NGINX**
* **3306:** MySQL / MariaDB
  * Podemos acceder con programas como **MySQL Workbench**, **phpMyAdmin** (a través del navegador), **DBeaver**, **TablePlus**, **HeidiSQL**, etc.
  * **Qué es**: son bases de datos relacionales muy populares, orientadas a velocidad en operaciones de lectura y escritura simples.
* **5432:** PostgreSQL
  * Podemos acceder con el programa **PGAdmin**, o directamente con **python** (SQLAlchemy)
  * **Qué es**: es una base de datos relacional más avanzada, con soporte nativo para datos complejos, transacciones completas y funciones más sofisticadas

Los puertos son como “puertas de entrada” a servicios específicos del servidor. Para conectarse correctamente, nuestro ordenador necesita “abrir la puerta” adecuada. Esa puerta puede accederse desde distintos lugares (aplicaciones o programas) de nuestro ordenador personal.

## NGINX como el portero del servidor al que accedemos desde nuestro navegador web

**NGINX** es un **servidor web** y **proxy reverso** (= un servidor que se coloca delante de uno o varios servidores internos y actúa como intermediario para recibir solicitudes de los clientes y enviarlas al servidor adecuado). Sus funciones principales son:

1. **Servir páginas web**
   * Recibe solicitudes de los navegadores (o clientes) y entrega archivos HTML, CSS, JS, imágenes, etc.
   * Es muy eficiente y rápido, capaz de manejar miles de conexiones simultáneas.
2. **Proxy reverso y balanceo de carga**
   * Puede recibir solicitudes y enviarlas a otros servidores (por ejemplo, un servidor de aplicaciones).
   * Distribuye la carga entre varios servidores para mejorar rendimiento y disponibilidad.
3. **Gestión de seguridad y certificados**
   * Maneja HTTPS (certificados SSL/TLS) para cifrar las comunicaciones.

A **NGINX**, como servidor web, se accede principalmente desde estos **puertos estándar** :

| Puerto | Protocolo | Uso                             |
| ------ | --------- | ------------------------------- |
| 80     | HTTP      | Páginas web sin cifrado        |
| 443    | HTTPS     | Páginas web cifradas (SSL/TLS) |

Opcionalmente, NGINX puede configurarse para escuchar en **otros puertos**, pero 80 y 443 son los más comunes.

## ¿Porqué utilizamos un Dockerfile en los programas del servidor?

Un **Dockerfile** es un archivo de texto que contiene las instrucciones para crear un **contenedor**.
Es una manera de disponer de entornos aislados en las aplicaciones o programas dentro del servidor, para aislarlos
y asegurar un funcionamiento consistente a lo largo del tiempo (cada aplicación o programa puede tener
sus propias dependencias, versiones, etc.)

Básicamente un Dockerfile le dice al servidor:

* Qué sistema base usar.
* Qué programas instalar.
* Cómo configurar el entorno.
* Cómo arrancar la aplicación.

Es como una **receta** para preparar tu aplicación.

Normalmente, cada **programa / aplicación / servicio** suele tener **su propio Dockerfile.**
En nuestro servidor, todo programa se ejecutará en un contenedor aislado. Las razones son:

* Usa un lenguaje distinto (Node, Python, Java, etc.)
* Tiene dependencias distintas
* Tiene su propia forma de arrancar

Los puntos a favor de utilizar un Dockerfile son:

* **Mismo entorno en todos lados**
  Garantiza que tu aplicación funcione igual en tu PC, en el servidor y en producción.
* **Despliegue fácil y repetible**
  El servidor no se configura a mano: el Dockerfile define todo.
* **Aislamiento de aplicaciones**
  Cada programa y base de datos corre sin interferir con otros.
* **Control de versiones**
  Fija versiones exactas de sistema, lenguaje y dependencias.
* **Actualizaciones seguras**
  Puedes actualizar o volver atrás sin romper el servidor.
* **Separación entre software y datos**
  Las bases de datos mantienen sus datos aunque el contenedor se borre.
* **Recuperación rápida**
  Si el servidor falla, se reconstruye todo en minutos.
* **Configuración como código**
  El Dockerfile documenta y versiona cómo se ejecuta tu app.

### Ejemplo del Dockerfile dentro del programa de Python para crear el visualizador

#### ¿Cómo funciona?

1. **El contenedor se inicia** a partir de la imagen ligera de Python 3.13 (`python:3.13-slim`).
2. Se crea un **usuario no root** para ejecutar la aplicación de manera segura dentro del contenedor.
3. Se instalan las **librerías de PostgreSQL** (`libpq-dev`) para que la aplicación pueda conectarse a bases de datos.
4. La **herramienta `uv`** se encarga de gestionar las dependencias de Python y de ejecutar la aplicación.
5. Se copian el **código fuente** y los **archivos de configuración de Streamlit** al contenedor.
6. El **ENTRYPOINT** ejecuta `uv run streamlit run` con tu archivo principal (`src/main.py`).
7. Se **expone el puerto 8502** , lo que permite acceder a la aplicación Streamlit desde el navegador en `http://localhost:8502`.

```
FROM python:3.13-slim

ARG USERNAME
ARG USER_UID
ARG USER_GID

WORKDIR /series_ui

EXPOSE 8502

# create the nonroot user
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

# install pgsql driver
RUN apt-get update
RUN apt-get install libpq-dev -y


# install uv
COPY --from=docker.io/astral/uv:latest /uv /uvx /bin/  

# copy and install requirements first as dependencies change far less often than the source code
COPY .python-version .python-version
COPY pyproject.toml pyproject.toml
#COPY uv.lock uv.lock
RUN uv sync
# RUN pip install --no-cache-dir -r requirements.txt

COPY src src

# chown once the workdir is full to avoid permission errors
RUN chown -R $USER_UID:$USER_UID . 
RUN mkdir .streamlit
COPY config.toml .streamlit/config.toml
COPY credentials.toml .streamlit/credentials.toml

ENTRYPOINT [ "uv", "run", "streamlit", "run" ]

# just keep the container alive
CMD [ "src/main.py" ]
#CMD [ "sleep", "infinity" ]
```

#### Flujo explicado:

1. **SSH** :

* Te permite abrir la terminal dentro del contenedor.
* Puedes ejecutar scripts, instalar paquetes o administrar la app directamente.

2. **Navegador Web** :

* Accedes a la app de Streamlit por `http://localhost:8502`.
* El contenedor recibe la solicitud y `uv` ejecuta `streamlit run src/main.py`. En este momento empieza a ejecutarse el código de python que permite ver la aplicación web vía streamlit.

3. **Dentro del contenedor** :

* Python y sus dependencias ya están instaladas mediante `uv sync`.
* Las librerías de PostgreSQL permiten que la app se conecte a bases de datos si es necesario.
* El código fuente y la configuración de Streamlit están listos para ejecutar la app.

```
[Tu ordenador / Cliente]
   ├── SSH (Puerto 22) ───────────────▶ [Contenedor Docker]
   │                                     ┌─────────────────────────────┐
   │                                     │       Contenedor Docker      │
   │                                     │  (Imagen: python:3.13-slim) │
   │                                     ├─────────────────────────────┤
   │                                     │ Directorio de trabajo:       │
   │                                     │ /series_ui                   │
   │                                     ├─────────────────────────────┤
   │                                     │ Usuario no root creado       │
   │                                     │ (USERNAME, UID, GID)        │
   │                                     ├─────────────────────────────┤
   │                                     │ Librerías PostgreSQL         │
   │                                     │ (libpq-dev)                  │
   │                                     ├─────────────────────────────┤
   │                                     │ Herramienta uv instalada     │
   │                                     │ (gestiona dependencias y env)│
   │                                     ├─────────────────────────────┤
   │                                     │ Dependencias Python         │
   │                                     │ instaladas mediante uv sync │
   │                                     ├─────────────────────────────┤
   │                                     │ Código fuente de la app      │
   │                                     │ (carpeta src/)               │
   │                                     ├─────────────────────────────┤
   │                                     │ Configuración de Streamlit   │
   │                                     │ (.streamlit/*.toml)          │
   │                                     ├─────────────────────────────┤
   │                                     │ ENTRYPOINT: uv run streamlit │
   │                                     │ CMD: src/main.py             │
   │                                     └─────────────┬───────────────┘
   │                                                   │
[Navegador Web]                                        │
   └── HTTP/HTTPS ─────────────────────────────────────┘
(Puerto 80 con una url que termina con "/series") 
             │
             ▼
  	como va por el puerto 80, la solicitud la pilla el portero nginx, que ve que acaba en "/series", y por tanto la manda al puerto 8502
             │
             ▼
	Por tanto vía NGINX la solicitud HTTP del navegador acaba redirigiéndose al puerto 8502 del servidor.
             │
             ▼
	La solicitud web llega al contenedor Docker (el contenedor está ejecutando: uv run streamlit run src/main.py)
	dentro del contenedor es Streamlit quien la recibe y la procesa
             │
             ▼
        uv ejecuta
      `streamlit run src/main.py`
             │
             ▼
      Streamlit ejecuta el código Python necesario para generar la respuesta
             │
             ▼
      (Opcional) Conexión a Base de Datos
        ┌─────────────┐       ┌─────────────┐
        │   MySQL     │       │ PostgreSQL  │
        │ (Puerto 3306)│       │ (Puerto 5432)│
        └─────────────┘       └─────────────┘
             │
             ▼
       Streamlit envía la respuesta de vuelta a través de Docker al navegador
             │
             ▼
     Navegador recibe la interfaz web

```

```
Navegador → Solicitud HTTP → Docker (puerto 80 --> NGINX --> Puerto 8502) → Streamlit (dentro del contenedor) → Python ejecuta código → Respuesta HTTP → Navegador
```
