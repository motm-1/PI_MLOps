import pandas as pd
import numpy as np
from etl_functions import read_json_file, parse_lists
from scraping_functions import scrape_missing_row

df_reviews = pd.read_csv('CleanDatasets/users_reviews.csv')
df_items = pd.read_csv('CleanDatasets/users_items.csv')
df_games = pd.read_csv('CleanDatasets/steam_games.csv', converters={'genres':parse_lists,'tags':parse_lists})


def exploded_df():
    df = df_items.copy()
    df = df.merge(df_games[['genres', 'id', 'release_date']], how='left', on='id')
    df.dropna(inplace=True)
    df['release_date'] = pd.to_datetime(df['release_date']).dt.year
    exploded_df = df.explode('genres')
    exploded_df = exploded_df[exploded_df.genres.str.strip() != '']

    return exploded_df 

def df_playtimegenre(df):
    df = df.groupby(['genres', 'release_date'])
    df = df['playtime_forever'].sum()
    df = df.reset_index()

    genres = list(pd.unique(df['genres']))
    ids = []
    for genre in genres:
        ids.append(df[df['genres'] == genre]['playtime_forever'].idxmax())
    df = df.loc[ids]

    df.to_parquet('ApiDatasets/playtimegenre.parquet', index=False)

def df_userforgenre(df):
    df_to_get_played_hours = df.groupby(['user_id', 'genres', 'release_date'])
    df_to_get_played_hours = df_to_get_played_hours['playtime_forever'].sum().reset_index()

    df_to_get_user_id = df.groupby(['user_id', 'genres'])
    df_to_get_user_id = df_to_get_user_id['playtime_forever'].sum().reset_index()
    
    genres = list(pd.unique(df_to_get_user_id['genres']))

    ids = []
    for genre in genres:
        ids.append(df_to_get_user_id[df_to_get_user_id['genres'] == genre]['playtime_forever'].idxmax())
    df_to_get_user_id = df_to_get_user_id.loc[ids, ['user_id', 'genres']].reset_index(drop=True)
    
    df = pd.DataFrame()

    for row in df_to_get_user_id.itertuples():
        genre = row.genres
        id = row.user_id
        df = pd.concat((df, df_to_get_played_hours[(df_to_get_played_hours['genres'] == genre) & (df_to_get_played_hours['user_id'] == id)]))

    df.to_parquet('ApiDatasets/userforgenre.parquet', index=False)

def main():
    df = exploded_df()
    df_playtimegenre(df)
    df_userforgenre(df)

if __name__ == "__main__":
    main()