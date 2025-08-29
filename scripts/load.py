from modules.db_loader import Load
import pandas as pd

# Inicializa la clase Load encargada de cargar DataFrames a MySQL
load = Load()

# Lectura del CSV que contiene las posiciones de los equipos en las ligas
team_positions_leagues_df = pd.read_csv('../data/team_positions_leagues.csv')  
print("LECTURA DEL CSV CON LAS LIGAS CON LAS POSICIONES DE EQUIPO")

# Carga el DataFrame en la tabla 'team_positions_leagues' de la base de datos 'espn_leagues_db'
load.load_to_mysql(
    team_positions_leagues_df,
    'team_positions_leagues',
    'espn_leagues_db'
)
print("TERMINA LA DISPONIBILIZACIÓN A MYSQL")
