import os  
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Carga las variables de entorno definidas en el archivo .env
load_dotenv()

def get_mysql_connection():
    """
    Crea y devuelve una conexión a MySQL usando SQLAlchemy.
    """

    # Obtiene la cadena de conexión desde la variable de entorno MYSQL_CONNECTION_STRING
    connection_string = os.getenv("MYSQL_CONNECTION_STRING")

    # Crea un motor (engine) de SQLAlchemy para conectarse a la base de datos MySQL
    engine = create_engine(connection_string)

    # Devuelve el engine para interactuar con la base de datos
    return engine


