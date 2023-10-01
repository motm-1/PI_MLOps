import pandas as pd
from etl_functions import parse_lists
from scraping_functions import scrape_missing_row

df_reviews = pd.read_csv('CleanDatasets/users_reviews.csv', parse_dates=['posted'])
df_items = pd.read_csv('CleanDatasets/users_items.csv')
df_games = pd.read_csv('CleanDatasets/steam_games.csv', converters={'genres':parse_lists,'tags':parse_lists})
df_sentiment = pd.read_csv('CleanDatasets/users_sentiment.csv')

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

def df_user_recommendations():
    df_r = df_reviews.copy()
    df_s = df_games.copy()
    df_i = df_items.drop_duplicates(subset='item_name')

    df_r['year'] = df_r.posted.dt.year
    df_r.drop(columns=['review', 'posted'], inplace=True)

    df = df_r.groupby(['year','recommend', 'item_id']).count().reset_index()
    years = pd.unique(df['year'])

    df_true = df[df['recommend'] == True]
    df_false = df[df['recommend'] == False]

    final_true_df = pd.DataFrame()
    final_false_df = pd.DataFrame()

    for year in years:
        final_true_df = pd.concat((final_true_df, df_true[df_true['year'] == year].sort_values(by='user_id', ascending=False).iloc[:3]))
        final_false_df = pd.concat((final_false_df, df_false[df_false['year'] == year].sort_values(by='user_id', ascending=False).iloc[:3]))

    final_true_df = final_true_df.merge(df_s[['id', 'title']],how='left', left_on='item_id', right_on='id').drop(['item_id', 'id', 'recommend', 'user_id'], axis=1).reset_index(drop=True)

    #Merge our diferent datasets to get the games titles
    final_false_df_1 = final_false_df.merge(df_s[['id', 'title']],how='left', left_on='item_id', right_on='id').drop(['item_id', 'id', 'recommend', 'user_id'], axis=1).reset_index(drop=True)
    final_false_df_2 = final_false_df.merge(df_i[['id', 'item_name']],how='left', left_on='item_id', right_on='id').drop(['item_id', 'id', 'recommend', 'user_id'], axis=1).reset_index(drop=True)
    final_false_df = final_false_df_1.merge(final_false_df_2, how='right', right_index=True, left_index=True).drop('year_y', axis=1).rename({'year_x':'year'}, axis=1)
    final_false_df['title'] = final_false_df.apply(lambda x: x['item_name'] if str(x['title']) == 'nan' else x['title'], axis=1)
    final_false_df.drop('item_name', axis=1, inplace=True)

    final_true_df['title'] = final_true_df['title'].str.strip()
    final_false_df['title'] = final_false_df['title'].str.strip()

    pattern = list(range(1, 4)) * (len(final_true_df) // 3)
    final_true_df['position'] = pattern  #Set the position in the ranking for every year
    
    pattern = list(range(1, 4)) * (len(final_false_df) // 3)
    final_false_df['position'] = 1
    final_false_df.loc[1:, 'position'] = list(range(1, 4)) * (len(final_false_df) // 3) #Set the position in the ranking for every year (the first year doesn't have three games to rank, only one)

    final_true_df.to_parquet('ApiDatasets/usersrecommend.parquet')
    final_false_df.to_parquet('ApiDatasets/usersnotrecommend.parquet')

def df_collaborative_filtering():
    df_s = df_games.copy()
    df_se = df_sentiment.copy()

    df_se = df_se.drop(columns=['recommend', 'posted'])

    df_merged = df_se.merge(df_s[['id', 'title','genres','tags','price','developer','release_date']], right_on='id', left_on='item_id', how='left')

    missing_rows = df_merged[df_merged['id'].isna()].drop_duplicates(subset='item_id')
    missing_rows['url'] = missing_rows.apply(lambda x: 'https://store.steampowered.com/app/' + str(x['item_id']), axis=1)

    df = scrape_missing_row(missing_rows)

    df['genres'] = df['genres'].apply(parse_lists)
    df['tags'] = df['tags'].apply(parse_lists)
    df = df.drop(['id', 'url'], axis=1).dropna(subset='title')
    
    df_to_replace = pd.merge(df_merged.loc[df_merged.isna().any(axis=1),'item_id'], df[['item_id','title','genres','tags','price','developer','release_date']], on='item_id', how='left')
    index = df_merged.loc[df_merged.isna().any(axis=1)].index
    df_to_replace.index = index
    df_merged.loc[df_merged.isna().any(axis=1),['title','genres','tags','price','developer','release_date']] = df_to_replace

    df_merged['release_date'] = pd.to_datetime(df_merged['release_date'], format='mixed', errors='coerce')
    df_merged.dropna(subset=['title'], inplace=True)
    df_merged['price'] = df_merged['price'].fillna(0)

    df_merged['labels'] = df_merged.apply(combine_columns, axis=1)
    df_merged.dropna(subset=['labels','developer'], inplace=True)
    df_merged['labels'] = df_merged['labels'].apply(lambda x: list(set(x)))
    df_merged.drop(['genres', 'tags', 'release_date', 'id'], axis=1, inplace=True)

    df_merged = df_merged[['user_id', 'title', 'item_id', 'price', 'developer', 'labels', 'sentiment_analysis']]

    df_merged.to_parquet('CleanDatasets/collaborative_filtering.parquet')

def df_sentiment_analysis():
    df_se = df_sentiment.copy()
    df_se.drop(['recommend', 'user_id', 'item_id'], axis=1, inplace=True)
    df_se['year'] = pd.to_datetime(df_se['posted']).dt.year
    df_se = df_se.groupby(['year', 'sentiment_analysis']).agg('count').reset_index().rename({'posted':'count'}, axis=1)

    df_se.to_parquet('ApiDatasets/sentimentanalysis.parquet')

def combine_columns(row):
    if row['genres'] is not None and not isinstance(row['genres'], str) and row['tags'] is not None and not isinstance(row['tags'], str):
        return row['genres'] + row['tags']
    elif row['genres'] is not None and not isinstance(row['genres'], str):
        return row['genres']
    elif row['tags'] is not None and not isinstance(row['tags'], str):
        return row['tags']
    else:
        return None

def main():
    df = exploded_df()
    df_playtimegenre(df)
    df_userforgenre(df)
    df_user_recommendations()

    df_sentiment_analysis()

if __name__ == "__main__":
    main()