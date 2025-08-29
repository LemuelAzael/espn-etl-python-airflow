import pandas as pd
from modules.web_scraping import get_leagues_data

class Transform:
    def __init__(self) -> None:
        self.process = 'Transform Process'

    def get_all_leagues_data(self, raw_leagues_df: pd.DataFrame) -> pd.DataFrame:
        """
        Itera sobre las ligas y obtiene datos concatenados en un solo DataFrame.
        """
        all_leagues = []
        for _, row in raw_leagues_df.iterrows():
            league_df = get_leagues_data(row['URL'], row['LIGA'])
            all_leagues.append(league_df)
        return pd.concat(all_leagues, ignore_index=True)

    def merge_with_teams(self, all_leagues_df: pd.DataFrame, team_table_df: pd.DataFrame) -> pd.DataFrame:
        """
        Une los datos de ligas con la tabla de equipos.
        """
        merge_df = pd.merge(all_leagues_df, team_table_df, how='inner', on='EQUIPO')
        merge_df = merge_df[
            ['ID_TEAM', 'EQUIPO', 'J', 'G', 'E', 'P', 'GF', 'GC', 'DIF', 'PTS', 'LIGA', 'CREATED_AT']
        ].copy()
        return merge_df

    def data_processing(self, raw_leagues_df: pd.DataFrame, team_table_df: pd.DataFrame, output_path: str = None) -> pd.DataFrame:
        """
        Función envoltura que ejecuta todo el flujo: 
        1) obtiene datos de ligas 
        2) los une con tabla de equipos 
        3) guarda CSV opcionalmente.
        """
        all_leagues_df = self.get_all_leagues_data(raw_leagues_df)
        final_df = self.merge_with_teams(all_leagues_df, team_table_df)
        if output_path:
            final_df.to_csv(output_path, index=False)
        return final_df

