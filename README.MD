# ⚽ Pipeline ETL de Posiciones de Ligas de Fútbol con Python, Airflow y MySQL

Este proyecto implementa un **pipeline ETL** para automatizar la extracción, transformación y carga de posiciones de equipos de siete ligas de fútbol desde ESPN hacia una base de datos MySQL, orquestado con **Apache Airflow** y desplegado en **Docker**.


## 🧠 Descripción General

El pipeline obtiene diariamente las posiciones de las ligas de fútbol desde las tablas públicas de ESPN, realiza la limpieza, transformación y enriquecimiento con metadatos de los equipos, y almacena el resultado final en MySQL. Esto permite consultas estructuradas, análisis exploratorios y potencial integración con herramientas de visualización de datos.


## 🏗️ Arquitectura del Proyecto

El flujo de trabajo está compuesto por scripts y módulos locales, con un DAG de Airflow que orquesta la ejecución secuencial de las etapas de **Extract & Transform** y **Load**, garantizando reproducibilidad y modularidad.

Secuencia de procesamiento:

1. **Data Source**

   * URLs de las ligas en ESPN (7 ligas).
   * Ejemplo: `https://www.espn.com.co/futbol/posiciones/_/liga/eng.1`

2. **Extract & Transform**

   * Scripts: `extract_transform.py` → `league_processing.py` → `web_scraping.py`
   * Archivos locales en `data/`:

     * `df_ligas.csv` → URLs y nombres de ligas.
     * `team_table.csv` → Catálogo de equipos con ID único.
   * Funciones: scraping, limpieza, concatenación y merge con metadatos de equipos.
   * Resultado: `team_positions_leagues.csv`.

3. **Load**

   * Scripts: `load.py` → `db_loader.py` → `connections.py`
   * Función: carga del dataset final a MySQL en la tabla `team_positions_leagues`.

4. **Orchestration**

   * DAG: `etl_espn_dag.py` (Airflow)
   * Función: ejecuta de forma secuencial los scripts de extracción-transformación y carga a MySQL.
   * Entorno Dockerizado para reproducibilidad y escalabilidad.


## 🧰 Tecnologías y Herramientas

* Lenguaje: Python 3.x
* Orquestación: Apache Airflow
* Base de Datos: MySQL
* Contenedores: Docker y Docker Compose
* Librerías: Pandas, SQLAlchemy, Requests
* Archivos de entrada/salida: CSV (`df_ligas.csv`, `team_table.csv`, `team_positions_leagues.csv`)


## 📁 Estructura del Repositorio

```bash
espn-etl-python-airflow/
├── dags/
│   └── etl_espn_dag.py        # DAG de Airflow
├── data/
│   ├── df_ligas.csv           # URLs y nombres de ligas
│   ├── team_positions_leagues.csv  # Output final del pipeline
│   └── team_table.csv         # Equipos con ID único
├── modules/
│   ├── __init__.py
│   ├── db_loader.py           # Lógica de carga a MySQL
│   ├── league_processing.py   # Procesamiento y concatenación de ligas
│   └── web_scraping.py        # Extracción y limpieza desde ESPN
├── notebooks/
│   ├── extract_draw.ipynb     # Notebook exploratorio de extracción
│   └── process_draw.ipynb     # Notebook exploratorio de procesamiento
├── scripts/
│   ├── extract_transform.py   # Script maestro de extracción y transformación
│   └── load.py                # Script maestro de carga
├── utils/
│   └── connections.py         # Configuración de conexión a MySQL
├── .dockerignore
├── .env-ejemplo               # Variables de entorno (ejemplo)
├── docker-compose.yml         # Orquestación con Docker
├── Dockerfile                 # Imagen base del proyecto
└── requirements.txt           # Dependencias del proyecto
```


## ⚙️ Instrucciones de Uso

1. Clonar el repositorio:

```bash
git clone https://github.com/<tu-usuario>/espn-etl-python-airflow.git
cd espn-etl-python-airflow
```

2. Configurar variables de entorno:

```bash
cp .env-ejemplo .env
```

3. Levantar el entorno Docker:

```bash
docker-compose up --build
```

4. Acceder a Airflow Web UI: `http://localhost:8080`
5. Ejecutar el DAG **`etl_espn_dag`** para procesar y cargar los datos.
6. Verificar que la tabla `team_positions_leagues` en MySQL contiene el dataset final.


## 🔄 Pipeline de Datos

| Etapa           | Script / Módulo                          | Descripción                                                              |
| --------------- | ---------------------------------------- | ------------------------------------------------------------------------ |
| Fuente de datos | ESPN                                     | Extrae posiciones de equipos de 7 ligas de fútbol desde tablas públicas. |
| Transformación  | extract\_transform.py                    | Orquesta extracción y transformación.                                    |
| Procesamiento   | league\_processing.py                    | Limpieza, concatenación de ligas y merge con catálogo de equipos.        |
| Scraping        | web\_scraping.py                         | Extrae y formatea datos de las tablas de ESPN.                           |
| Almacenamiento  | team\_positions\_leagues.csv             | Dataset final en formato CSV local.                                      |
| Carga a BD      | load.py / db\_loader.py / connections.py | Inserta datos en MySQL.                                                  |
| Orquestación    | etl\_espn\_dag.py                        | Ejecuta el pipeline de forma secuencial con Airflow.                     |


## 📊 Output

* `team_positions_leagues.csv` y la tabla MySQL contienen:

  * Liga
  * Equipo
  * Posición
  * Puntos, victorias, empates, derrotas, goles
  * Metadatos de equipos


## 🎯 Objetivos del Proyecto

* Implementar un pipeline ETL **modular y escalable**.
* Demostrar **web scraping, transformación y carga a base de datos**.
* Orquestar procesos con **Airflow** y asegurar reproducibilidad con **Docker**.
* Proveer un **dataset estructurado y enriquecido** listo para análisis o visualización.


## 👤 Autor
Lemuel Azael Carrillo Barrera
Proyecto para portafolio de ingeniería de datos.

🔗 [Más sobre mí](https://www.notion.so/Perfil-profesional-Lemuel-Azael-Carrillo-Barrera-2619ec6ab8528025b300d6099cd92add?source=copy_link)

## 📚 Desarrollo completo del proyecto

Revisa todo el proceso paso a paso: desde la configuración inicial hasta la ejecución automatizada en AWS.

🔎 [Ver desarrollo completo en Notion](https://www.notion.so/ETL-from-ESPN-Football-to-MySQL-using-Python-and-Airflow-2619ec6ab8528031b9fdd6cd18da6760?source=copy_link)

## 🌐 Portafolio completo

Explora más proyectos y recursos en mis portafolios personales:

🔗 [GitHub – Portafolio](https://github.com/LemuelAzael/LemuelAzael)
🔗 [Notion – Portafolio](https://www.notion.so/Portafolio-Lemuel-Carrillo-2179ec6ab8528029ba54f3bf3363f993?source=copy_link)
