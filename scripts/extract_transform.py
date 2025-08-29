import pandas as pd
from modules.league_processing import Transform

# Inicializa la clase Transform que contiene el flujo de procesamiento de datos
transform = Transform()

# Lectura de archivos CSV
raw_leagues_df = pd.read_csv('../data/df_ligas.csv')     # Datos de ligas y sus URLs
team_table_df = pd.read_csv('../data/team_table.csv')    # Datos de la tabla de equipos
print("LECTURA DE LOS ARCHIVOS CSV DE LA INFORMACIÓN DE LIGAS Y EQUIPOS")

# Ejecuta todo el flujo de procesamiento: extracción, limpieza, unión y guardado
transform.data_processing(
    raw_leagues_df,
    team_table_df,
    output_path='team_positions_leagues.csv'
)
print("TERMINA LA EXTRACCIÓN Y PROCESAMIENTO")
