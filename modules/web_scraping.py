import time
import random
import pandas as pd
from datetime import datetime

def get_leagues_data(url: str, liga: str) -> pd.DataFrame:
    """
    Obtiene datos de una liga desde una URL, hace limpieza inicial y añade metadatos.
    """
    tiempo = [1, 3, 2]
    time.sleep(random.choice(tiempo))

    df = pd.read_html(url)
    df = pd.concat([df[0], df[1]], ignore_index=True, axis=1)
    df = df.rename(
        columns={0: 'EQUIPO', 1: 'J', 2: 'G', 3: 'E', 4: 'P', 5: 'GF', 6: 'GC', 7: 'DIF', 8: 'PTS'}
    )

    df['EQUIPO'] = df['EQUIPO'].apply(
        lambda x: x[5:] if x[:2].isnumeric() else x[4:]
    )
    df['LIGA'] = liga

    run_date = datetime.now().strftime("%Y-%m-%d")
    df['CREATED_AT'] = run_date

    return df
