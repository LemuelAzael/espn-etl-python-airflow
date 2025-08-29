import pandas as pd                
from sqlalchemy import create_engine  

# Importa el módulo de conexiones definido en utils para MySQL
from utils import connections as c  


class Load:
    """
    Clase encargada de cargar DataFrames a la base de datos MySQL.
    """

    def __init__(self) -> None:
        # Define el nombre del proceso para fines de seguimiento
        self.process = 'Load Process'

    def load_to_mysql(self, df, tableName, databaseName):
        """
        Carga un DataFrame a una tabla MySQL específica.

        Parámetros:
        - df: DataFrame de pandas a cargar.
        - tableName: nombre de la tabla de destino.
        - databaseName: nombre de la base de datos (schema) de MySQL.
        """

        # Obtiene la conexión a MySQL usando la función definida en utils.connections
        engine = c.get_mysql_connection()

        # Inserta el DataFrame en la tabla especificada; si ya existe, se añade al final
        df.to_sql(
            name=tableName,
            schema=databaseName,
            con=engine,
            if_exists='append',
            index=True  # Incluye la columna de índices del DataFrame
        )
